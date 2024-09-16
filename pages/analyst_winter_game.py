import _snowflake
import json
import streamlit as st
import time
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import pandas as pd
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

APP_ID = 3
# Définition des constantes pour les modèles sémantiques
session = get_active_session()

# Définir la requête SQL pour obtenir les données
query = """
SELECT APP_NAME, APP_DATABASE, APP_SCHEMA, APP_STAGE
FROM CORTEX_DB.PUBLIC.CORTEX_APPS
WHERE APP_ID = 3
"""

results = session.sql(query).collect()
row = results[0]
APP_TITLE = row['APP_NAME']
DATABASE = row['APP_DATABASE']
SCHEMA = row['APP_SCHEMA']
STAGE = row['APP_STAGE']

def fetch_yamls():
    session = get_active_session()
    query = """
    SELECT *
    FROM CORTEX_DB.PUBLIC.CORTEX_MODELS
    WHERE APP_ID = 3
    AND CORTEX_YAML_ACTIVE = 1
    """
    results = session.sql(query).collect()
    FILES = {}
    for row in results:
        FILES[row['CORTEX_YAML_NAME']] = row['CORTEX_YAML_FILE']
    return FILES

FILES = fetch_yamls()

def log_to_snowflake(username, app_name, yaml_file, input_text, output_json, elapsed_time):
    session = get_active_session()
    session.sql("""
        INSERT INTO CORTEX_DB.PUBLIC.CORTEX_LOGS (DateTime, Username, App_Name, Yaml_File, input_text, output_json, elapsed_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(), 
        username, 
        app_name, 
        yaml_file, 
        input_text, 
        json.dumps(output_json),
        elapsed_time
    )).collect()

@st.cache_data(ttl=3600)
def fetch_key_questions(username):
    session = get_active_session()
    
    # Récupérer les bookmarks de l'utilisateur
    user_bookmarks_query = """
    SELECT bk_question 
    FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
    WHERE APP_ID = 3
    AND BK_USERNAME = 'ALL'
    ORDER BY BK_UPDATED_AT DESC
    LIMIT 6
    """
    user_bookmarks = session.sql(user_bookmarks_query).collect()

    if user_bookmarks:
        return [row['BK_QUESTION'] for row in user_bookmarks]
    else:
        # Si pas de bookmarks, récupérer les 4 questions les plus fréquentes
        top_questions_query = """
        SELECT INPUT_TEXT, COUNT(*) as count
        FROM CORTEX_DB.PUBLIC.CORTEX_LOGS
        WHERE APP_NAME = 'Winter Games'
        GROUP BY INPUT_TEXT
        ORDER BY count DESC
        LIMIT 4
        """
        top_questions = session.sql(top_questions_query).collect()
        return [row['INPUT_TEXT'] for row in top_questions]

def display_key_questions(username):
    st.markdown("<h2 style='text-align: center; color: #FFFFFF;'>Comment puis-je vous aider aujourd'hui ?</h2>", unsafe_allow_html=True)
    
    key_questions = fetch_key_questions(username)

    st.markdown("""
    <style>
    .stButton > button {
        width: 100%;
        background-color: #2E303E;
        color: white;
        border: 1px solid #4A4B5A;
        border-radius: 5px;
        padding: 15px;
        font-size: 16px;
        text-align: left;
        margin-bottom: 10px;
    }
    .stButton > button:hover {
        background-color: #3E3F4E;
    }
    </style>
    """, unsafe_allow_html=True)

    with st.container():
        col1, col2 = st.columns(2)
        for i, question in enumerate(key_questions):
            if i % 2 == 0:
                with col1:
                    if st.button(question, key=f"q_{i}_{hash(question)}"):
                        process_message(question)
            else:
                with col2:
                    if st.button(question, key=f"q_{i}_{hash(question)}"):
                        process_message(question)


def load_and_display_image(stage_path: str):
    session = get_active_session()
    try:
        # Utilisation de get_stream pour récupérer l'image depuis Snowflake stage
        with session.file.get_stream(stage_path) as file_stream:
            image_data = file_stream.read()  # Lecture du contenu du fichier image
            st.image(image_data, use_column_width=True)
    except Exception as e:
        st.error(f"Erreur lors du chargement de l'image : {e}")






def insert_bookmark_data(app_id, username, question, lang):
    session = get_active_session()
    try:
        query = """
        INSERT INTO CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        (APP_ID, BK_USERNAME, BK_QUESTION, BK_LANG)
        VALUES (?, ?, ?, ?)
        """
        session.sql(query, (
            app_id,
            username,
            question,
            lang
        )).collect()
        logging.info(f"Bookmark ajouté pour l'utilisateur {username}: {question}")
        return True
    except Exception as e:
        logging.error(f"Erreur lors de l'ajout du bookmark: {str(e)}")
        return False

def add_bookmark_button(app_id, question, lang, message_index):
    question_hash = hashlib.md5(question.encode()).hexdigest()
    
    username_result = get_active_session().sql("SELECT CURRENT_USER()").collect()
    username = username_result[0][0]
    
    if st.button("🔖", key=f"bookmark_{message_index}_{question_hash}_{time.time()}"):
        success = insert_bookmark_data(
            app_id=app_id,
            username=username,
            question=question,
            lang=lang
        )
        if success:
            st.success("Question enregistrée dans vos favoris !")
        else:
            st.error("Erreur lors de l'enregistrement du favori. Veuillez vérifier les logs pour plus de détails.")

def send_message(prompt: str, yaml_file: str) -> dict:
    session = get_active_session()
    username_result = session.sql("SELECT CURRENT_USER()").collect()
    username = username_result[0][0]  

    start_time = time.time()

    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{yaml_file}",
    }

    try:
        resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )

        elapsed_time = int((time.time() - start_time) * 1000)

        if resp["status"] < 400:
            output_json = json.loads(resp["content"])
            log_to_snowflake(
                username=username,
                app_name="Winter Games",
                yaml_file=yaml_file,
                input_text=prompt,
                output_json=output_json,
                elapsed_time=elapsed_time
            )
            return output_json
        else:
            st.error(f"Erreur de l'API : {resp['status']} - {resp.get('content', 'Pas de détails')}")
            return None
    except Exception as e:
        st.error(f"Une erreur est survenue : {str(e)}")
        return None

def process_message(prompt: str) -> None:
    APP_PREFIX = 'winter_game_'
    yaml_file = FILES[st.session_state[f'{APP_PREFIX}selected_model']]
    st.session_state[f'{APP_PREFIX}messages'].append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
        add_bookmark_button(app_id=APP_ID, question=prompt, lang="FR", message_index=len(st.session_state[f'{APP_PREFIX}messages'])-1)
    with st.chat_message("assistant"):
        with st.spinner("Génération de la réponse..."):
            response = send_message(prompt=prompt, yaml_file=yaml_file)
            if response:
                content = response["message"]["content"]
                display_content(content=content)
                st.session_state[f'{APP_PREFIX}messages'].append({"role": "assistant", "content": content})

def display_content(content: list, message_index: int = None) -> None:
    APP_PREFIX = 'winter_game_'
    message_index = message_index or len(st.session_state[f'{APP_PREFIX}messages'])
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    unique_key = f"{message_index}_{suggestion_index}_{hash(suggestion)}_{time.time()}"
                    if st.button(suggestion, key=f"suggestion_{unique_key}"):
                        st.session_state[f'{APP_PREFIX}active_suggestion'] = suggestion
        elif item["type"] == "sql":
            with st.expander("Requête SQL", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Résultats", expanded=True):
                with st.spinner("Exécution de la requête SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()
                    if not df.empty:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["Données", "Graphique en ligne", "Graphique en barres"]
                        )
                        data_tab.dataframe(df)

                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])

                            df_numeric = df.apply(pd.to_numeric, errors='coerce')
                            df_numeric = df_numeric.dropna(axis=1, how='all')

                            with line_tab:
                                st.line_chart(df_numeric)
                            with bar_tab:
                                st.bar_chart(df_numeric)
                        else:
                            st.info("Le DataFrame n'a pas assez de colonnes pour générer un graphique.")

                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="Télécharger les résultats en CSV",
                            data=csv,
                            file_name="resultats_requete.csv",
                            mime="text/csv",
                            key=f"download_{message_index}_{hash(df.to_string())}_{time.time()}"
                        )
                    else:
                        st.info("Aucun résultat trouvé pour cette requête.")





def main():
    APP_PREFIX = 'winter_game_' 
    if f'{APP_PREFIX}selected_model' not in st.session_state:
        st.session_state[f'{APP_PREFIX}selected_model'] = list(FILES.keys())[0]

    previous_model = st.session_state[f'{APP_PREFIX}selected_model']
    
    st.session_state[f'{APP_PREFIX}selected_model'] = st.selectbox(
        "Choisissez un modèle sémantique",
        list(FILES.keys()),
        key="{APP_PREFIX}model_selector",
        index=list(FILES.keys()).index(st.session_state[f'{APP_PREFIX}selected_model'])
    )

    if previous_model != st.session_state[f'{APP_PREFIX}selected_model']:
        st.session_state[f'{APP_PREFIX}messages'] = []
        st.session_state[f'{APP_PREFIX}suggestions'] = []
        st.session_state[f'{APP_PREFIX}active_suggestion'] = None
 

    st.markdown(f"<h1 style='text-align: center; color: #FFFFFF;'>{st.session_state[f'{APP_PREFIX}selected_model']} Model</h1>", unsafe_allow_html=True)

    stage_path = '@"CORTEX_ANALYST_DEMO"."WINTER_GAME"."RAW_DATA"/logo_winter_game.png'
    app_logo_url = load_and_display_image(stage_path)

    username_result = get_active_session().sql("SELECT CURRENT_USER()").collect()
    current_username = username_result[0][0]
    display_key_questions(current_username)

    if f'{APP_PREFIX}messages' not in st.session_state:
        st.session_state[f'{APP_PREFIX}messages'] = []
        st.session_state[f'{APP_PREFIX}suggestions'] = []
        st.session_state[f'{APP_PREFIX}active_suggestion'] = None

    for message_index, message in enumerate(st.session_state[f'{APP_PREFIX}messages']):
        with st.chat_message(message["role"]):
            display_content(content=message["content"], message_index=message_index)

    if user_input := st.chat_input("Quelle est votre question ?"):
        process_message(prompt=user_input)

    if st.session_state[f'{APP_PREFIX}active_suggestion']:
        process_message(prompt=st.session_state[f'{APP_PREFIX}active_suggestion'])
        st.session_state[f'{APP_PREFIX}active_suggestion'] = None

if __name__ == "__main__":
    main()
