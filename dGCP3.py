

# librerÃ­as
import streamlit as st
import base64  
import pandas as pd

from google.cloud import bigquery 
from google.oauth2 import service_account
import pandas as pd
import numpy as np 
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import pandas_gbq as gbq
import datetime 
import time
import os
import json
from google.oauth2.credentials import Credentials
# lectura


# Create credentials object from loaded information
credentials = Credentials(
    token=None,
    refresh_token=st.secrets.get("REFRESH_TOKEN"),
    token_uri="https://oauth2.googleapis.com/token",
    client_id=st.secrets.get("CLIENT_ID"),
    client_secret=st.secrets.get("CLIENT_SECRET"),
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Create BigQuery client using authorized user credentials
client = bigquery.Client(credentials=credentials)


# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
#@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    df = query_job.to_dataframe() # last add
    return df

rows = run_query("SELECT * FROM `tc-sc-bi-bigdata-visua-sf-wbx.raw_tables.raw_sla_adquisiciones` LIMIT 10")

# definiendo secciones

# layout
st.title("Portal de Drivers Servicios Falabella")

# funciones de secciones
def download_section():
    """
    Seccion para descargar archivos
    """
    # Obtiene el archivo que se desea descargar
    filename = st.selectbox("Seleccione el archivo que desea descargar", ["archivo1.csv", "archivo2.txt"])

    # Mostrar el nombre del archivo
    st.write(f"Descargar {filename}")

    # Boton de descarga
    download_button = st.button("Descargar")
    if download_button:
        # Descarga el archivo
        with open(filename, "rb") as file:
            file_content = file.read()
            base64_file = base64.b64encode(file_content).decode('utf-8')
            href = f'<a href="data:file/{filename.split(".")[-1]};base64,{base64_file}" download="{filename}">Haz clic aqui para descargar</a>'
            st.markdown(href, unsafe_allow_html=True)
            
            
def upload_section():
    """
    Seccion para subir archivos
    """
    # Obtiene el archivo que se desea subir
    file = st.file_uploader("Seleccione el archivo que desea subir", type=["csv", "txt", "xlsx"])

    # Sube el archivo a BigQuery
    if file is not None:
        df = pd.read_excel(file)  # Assuming CSV format

        try:
            dataset_id = "raw_tables"  # Replace with your dataset ID
            table_id = "test_drivers"  # Replace with your table ID
            table_ref = client.dataset(dataset_id).table(table_id)

            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for the job to complete

            st.success("Archivo subido exitosamente a BigQuery!")
        except Exception as e:
            st.error(f"Error al subir el archivo: {e}")
        
def results_section():
    """
    Seccion para visualizar resultados
    """
    # Visualiza los resultados
    st.write(rows.head(5))
 
# seccionando la pagina
st.sidebar.title("Menu")
st.sidebar.subheader("Seleccione la seccion que desea ver")
selected_section = st.sidebar.radio("Seccion", ["Descargar archivos", "Subir archivos", "Visualizar resultados"], key="section")

if selected_section == "Descargar archivos":
    download_section()
elif selected_section == "Subir archivos":
    upload_section()
elif selected_section == "Visualizar resultados":
    results_section()