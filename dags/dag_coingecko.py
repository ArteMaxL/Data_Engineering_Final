from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import os

# Path base archivo config.json
configPath = os.path.join(
    os.getenv("AIRFLOW_HOME", ""), "dags/keys/config.json"
)

# Lee el archivo JSON
with open(configPath, 'r') as file:
    config = json.load(file)

# Seteo de variables por su clave
redshift_host = config['REDSHIFT_HOST']
redshift_port = config['REDSHIFT_PORT']
redshift_dbname = config['REDSHIFT_DBNAME']
redshift_user = config['REDSHIFT_USER']
redshift_password = config['REDSHIFT_PASSWORD']

email_host = config['EMAIL_HOST']
email_port = config['EMAIL_PORT']
email_user = config['EMAIL_USER']
email_password = config['EMAIL_PASSWORD']
email_destination = config['EMAIL_DESTINATION']

def extract_coingecko_data():
    """
    Obtiene los datos de las 5 principales criptomonedas desde CoinGecko.

    Returns:
    list: Una lista de diccionarios con información de las criptomonedas.
    """
    response = requests.get('https://api.coingecko.com/api/v3/coins/markets', params={'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': '5', 'page': '1', 'sparkline': 'false'})
    top_coins = response.json()
    return top_coins

def transform_data(**kwargs):
    """
    Transforma los datos de las criptomonedas extraídos.

    Args:
    **kwargs: Argumentos adicionales.

    Returns:
    list: Una lista de diccionarios con la información transformada de las criptomonedas.
    """
    extracted_data = kwargs['task_instance'].xcom_pull(task_ids='extract_coingecko_data')
    print(extracted_data)
    transformed_data = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for coin_data in extracted_data:
        coin_info = {
            'nombre': coin_data['name'],
            'precio': coin_data['current_price'],
            'market_cap': coin_data['market_cap'],
            'fecha_obtencion': current_time
        }
        transformed_data.append(coin_info)
    
    return transformed_data

# Definir umbrales configurables
precio_threshold = 50000            # Precio cripto
market_cap_threshold = 1000000000   # Umbral de capitalización

def load_to_redshift(**kwargs):
    """
    Carga datos en Redshift y envía alertas por correo electrónico basadas en umbrales.

    Args:
    **kwargs: Argumentos adicionales.

    Raises:
    Exception: Se produce una excepción si hay un problema durante la carga de datos o el envío de alertas por correo electrónico.
    """
    transformed_data = kwargs['task_instance'].xcom_pull(task_ids='transform_data')
    conn = psycopg2.connect(
        dbname = redshift_dbname,
        host = redshift_host,
        port = redshift_port,
        user = redshift_user,
        password = redshift_password
    )
    cursor = conn.cursor()
    
    for asset_info in transformed_data:
        nombre = asset_info['nombre']
        precio = asset_info['precio']
        market_cap = asset_info['market_cap']
        fecha_obtencion = asset_info['fecha_obtencion']
        
        insert_query = f"INSERT INTO datos_coingecko (nombre, precio, market_cap, fecha_obtencion) VALUES ('{nombre}', {precio}, {market_cap}, '{fecha_obtencion}')"
        cursor.execute(insert_query)
    
    conn.commit()
    conn.close()

    # Obtener la fecha y hora actual
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Verificar umbrales y enviar alertas por correo electrónico si se superan
    for asset_info in transformed_data:
        nombre = asset_info['nombre']
        precio = asset_info['precio']
        market_cap = asset_info['market_cap']

        if precio > precio_threshold:
            subject = f'Alerta - Precio alto de {nombre}'
            body = f'El precio de {nombre} ha superado el umbral de {precio_threshold}. Precio actual: {precio}. Fecha y hora: {current_datetime}'
            send_email(subject, body)

        if market_cap > market_cap_threshold:
            subject = f'Alerta - Market Cap alto de {nombre}'
            body = f'El Market Cap de {nombre} ha superado el umbral de {market_cap_threshold}. Market Cap actual: {market_cap}. Fecha y hora: {current_datetime}'
            send_email(subject, body)

    # Envío de correo electrónico después de la inserción
    subject = 'Datos insertados en Redshift'
    body = f'Los datos se han insertado correctamente en Redshift. Fecha y hora: {current_datetime}'
    send_email(subject, body)

def send_email(subject, body):
    """
    Envía un correo electrónico con el asunto y cuerpo especificados.

    Args:
    subject (str): Asunto del correo electrónico.
    body (str): Cuerpo del correo electrónico.

    Raises:
    smtplib.SMTPException: Se produce cuando hay un problema con la conexión SMTP o el envío del correo electrónico.
    """
    from_email = email_user         # Dirección de email de origen
    to_email = email_destination    # Email destinatario

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(email_host, email_port)
    server.starttls()
    server.login(email_password)

    server.send_message(msg)
    server.quit()

# Argumentos del DAG principal (ETL)
default_args = {
    'owner': 'Artemio',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG principal (ETL)
dag = DAG(
    'DAG_coingecko',
    default_args=default_args,
    description='ETL con CoinGecko',
    schedule_interval=timedelta(hours=1),  # Intervalo de 1 hora
)

# Operadores del DAG principal (ETL)
extract_data = PythonOperator(
    task_id='extract_coingecko_data',
    python_callable=extract_coingecko_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_to_redshift = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag,
)

# Ejecución de las tareas del DAG principal
extract_data >> transform_data >> load_to_redshift
