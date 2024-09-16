# cortex_streamlit
#manage connection to snowflake

snow connection add  --connection-name  (Name of the new connection) --accountname  (Account name to use when authenticating with Snowflake)--username (Username to connect to Snowflake) --password (Snowflake password [default: optional]) --role (Role to use on Snowflake. [default: optional]) --warehouse  (Warehouse to use on Snowflake[default: optional]) --database (Database to use on Snowflake. [default: optional]) --schema  (Schema to use on Snowflake. [default: optional])--host (Host name the connection attempts to connect to Snowflake)      

#Deploys a Streamlit app

snow streamlit deploy --connection-name
