# ETL con CoinGecko

Este proyecto consiste en un flujo de trabajo (DAG) utilizando Airflow para extraer, transformar y cargar datos de las principales criptomonedas obtenidas de CoinGecko en una base de datos Redshift. Además, se implementa una lógica de alertas por correo electrónico basadas en umbrales configurables.

## Uso del Proyecto

### Requisitos Previos

Asegúrate de tener Docker instalado en tu máquina.

## Configuración de Credenciales

Para configurar las credenciales necesarias, sigue estos pasos:

1. En la carpeta `keys`, agrega un archivo llamado `config.json`.
2. Dentro de `config.json`, proporciona las siguientes variables con tus propias credenciales:

```json
{
  "REDSHIFT_HOST": "tu-cluster-redshift.us-east-1.redshift.amazonaws.com",
  "REDSHIFT_PORT": 5439,
  "REDSHIFT_DBNAME": "tu-redshift-database",
  "REDSHIFT_USER": "tu_usuario_redshift",
  "REDSHIFT_PASSWORD": "tu_contraseña_redshift",
  "EMAIL_HOST": "smtp.gmail.com",
  "EMAIL_PORT": 587,
  "EMAIL_USER": "tu_correo@gmail.com",
  "EMAIL_PASSWORD": "tu_contraseña_correo",
  "EMAIL_DESTINATION": "correo_destino@gmail.com"
}
```

### Ejecución del Proyecto

1. Clona este repositorio en tu máquina local.

2. Abre una terminal en la carpeta del proyecto.

3. Ejecuta el siguiente comando para construir la imagen de Docker:
```bash
   docker compose build
```

4. Una vez que se complete la construcción, inicia el proyecto con:
```bash
   docker compose up
```

Esto iniciará el ambiente local en `localhost:8080`, donde podrás acceder a Airflow y ejecutar el DAG `DAG_coingecko`.

## Funciones Principales

### `extract_coingecko_data()`

Obtiene los datos de las 5 principales criptomonedas desde CoinGecko.

### `transform_data(**kwargs)`

Transforma los datos de las criptomonedas extraídos.

### `load_to_redshift(**kwargs)`

Carga datos en Redshift y envía alertas por correo electrónico basadas en umbrales.

### `send_email(subject, body)`

Envía un correo electrónico con el asunto y cuerpo especificados.

## Umbrales Configurables

- `precio_threshold = 50000`: Umbral de precio de criptomoneda.
- `market_cap_threshold = 1000000000`: Umbral de capitalización de mercado.

## DAG y Ejecución

El DAG principal (`DAG_coingecko`) está configurado para ejecutarse cada 1 hora y consta de tres tareas:

- `extract_coingecko_data`: Extrae los datos de CoinGecko.
- `transform_data`: Transforma los datos extraídos.
- `load_to_redshift`: Carga los datos transformados en Redshift y envía alertas por correo electrónico basadas en umbrales.
