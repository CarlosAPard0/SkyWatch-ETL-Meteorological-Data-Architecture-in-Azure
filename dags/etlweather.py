from airflow.decorators import dag, task # Uso de decoradores modernos
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

# --- Configuración ---
CITY_NAME = 'Cuenca_Azuay'
LATITUDE = '-2.9002'
LONGITUDE = '-79.0100'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'skywatch_admin',
    'retries': 2,
}

# En Airflow 2.4+ se prefiere 'schedule' sobre 'schedule_interval'
# El decorador @dag reemplaza la necesidad de instanciar DAG(...) al final
@dag(
    dag_id='skywatch_weather_intelligence',
    default_args=default_args,
    description='Pipeline de inteligencia meteorológica para monitoreo local',
    schedule='@hourly', 
    start_date=datetime(2024, 12, 20),
    catchup=False,
    tags=['production', 'weather', 'ecuador']
)
def skywatch_etl():

    @task
    def extract_raw_weather():
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        raise ValueError(f"Error en sensor: {response.status_code}")

    @task
    def refine_weather_data(raw_data: dict):
        current = raw_data.get('current_weather', {})
        is_windy = current.get('windspeed', 0) > 15 
        
        return {
            'city': CITY_NAME,
            'temp': current.get('temperature'),
            'wind_speed': current.get('windspeed'),
            'wind_dir': current.get('winddirection'),
            'condition_code': current.get('weathercode'),
            'obs_time': current.get('time'),
            'alert_flag': is_windy
        }

    @task
    def load_to_warehouse(data: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_logs (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temp FLOAT,
            wind_speed FLOAT,
            wind_dir FLOAT,
            condition_code INT,
            alert_flag BOOLEAN,
            recorded_at TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        insert_query = """
        INSERT INTO weather_logs (city, temp, wind_speed, wind_dir, condition_code, alert_flag, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                cur.execute(insert_query, (
                    data['city'], data['temp'], data['wind_speed'], 
                    data['wind_dir'], data['condition_code'], 
                    data['alert_flag'], data['obs_time']
                ))
                conn.commit()
        
        logging.info(f"Datos de {CITY_NAME} guardados correctamente.")

    # Flujo de ejecución simplificado por el decorador @dag
    raw_info = extract_raw_weather()
    clean_info = refine_weather_data(raw_info)
    load_to_warehouse(clean_info)

# Ejecutar la función para registrar el DAG en Airflow
skywatch_etl()