import streamlit as st
from snowflake.snowpark.context import get_active_session
from PIL import Image
import io

# Fonction pour charger une image depuis un stage Snowflake
def load_image_from_snowflake(stage_path):
    session = get_active_session()
    try:
        with session.file.get_stream(stage_path) as file_stream:
            image_data = file_stream.read()  # Lecture du contenu de l'image
            return Image.open(io.BytesIO(image_data))  # Retourner l'image sous forme de fichier PIL
    except Exception as e:
        st.error(f"Erreur lors du chargement de l'image : {e}")
        return None

# Titles for each app
apps = {
    "Winter Game App": {
        "page": "analyst_winter_game",
        "logo": '@"CORTEX_ANALYST_DEMO"."WINTER_GAME"."RAW_DATA"/logo_winter_game.png'
    },
    "St-Gobain App": {
        "page": "analyst_st_gobain",
        "logo": '@"CORTEX_ANALYST_DEMO"."REVENUE_TIMESERIES"."MY_STREAMLIT_STAGE"/Cortex_Analyst_Demo/logo st-gobain.png'
    },
    "Monitoring App": {
        "page": "monitoring",
        "logo": '@"CORTEX_ANALYST_DEMO"."REVENUE_TIMESERIES"."MY_STREAMLIT_STAGE"/Cortex_Analyst_Demo/logo monitoring.png'
    }
}

# Titre de l'application
st.title("Bienvenue sur Cortex ST-Gobain Apps")

# Amélioration visuelle de la mise en page
st.markdown(
    """
    <style>
    .stImage {
        border-radius: 15px;
        margin-bottom: 10px;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.2);
    }
    .stButton>button {
        width: 100%;
        background-color: #1E90FF; /* Couleur bleu similaire à Snowflake */
        color: white;
        padding: 10px;
        border-radius: 10px;
        border: none;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .stButton>button:hover {
        background-color: #4682B4;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }
    </style>
    """, 
    unsafe_allow_html=True
)

# Définir une taille uniforme pour les logos
logo_width = 200
logo_height = 200

# Affichage en grille avec deux colonnes
cols = st.columns(3)  # Crée trois colonnes pour un affichage en grille

    # Ajoutez ici le reste du contenu et des fonctionnalités de l'application 
# Boucle sur chaque app
for index, (app_name, app_info) in enumerate(apps.items()):
    col = cols[index % 3]  # Alterner entre les colonnes
    with col:
        # Charger l'image depuis le stage Snowflake
        logo_image = load_image_from_snowflake(app_info["logo"])
        if logo_image:  # Si l'image est bien chargée
            # Redimensionner l'image à une taille uniforme
            resized_image = logo_image.resize((logo_width, logo_height))
            if st.button(app_name):
                # Gestion d'erreur améliorée lors de la redirection
                try:
                    st.session_state.selected_page = app_info["page"]
                except Exception as e:
                    st.error(f"Erreur lors de la navigation vers l'application {app_name}: {e}")
            st.image(resized_image, use_column_width=False, caption=app_name)

# Gestion des redirections entre les applications
if "selected_page" in st.session_state:
    try:
        if st.session_state.selected_page == "analyst_winter_game":
            APP_PREFIX = 'winter_game_'
            import pages.analyst_winter_game
            pages.analyst_winter_game.main()
        elif st.session_state.selected_page == "analyst_st_gobain":
            APP_PREFIX = 'st_gobain_'
            import pages.analyst_st_gobain 
            pages.analyst_st_gobain.main()
        elif st.session_state.selected_page == "monitoring":
            import pages.monitoring
    except ModuleNotFoundError as e:
        st.error(f"Le module pour l'application sélectionnée n'a pas été trouvé: {e}")
        st.info("Retourner à la page d'accueil en sélectionnant une autre application.")
