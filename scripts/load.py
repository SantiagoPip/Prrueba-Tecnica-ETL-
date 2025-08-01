import pandas as pd
from sqlalchemy import create_engine
import os

def load_to_postgres():
    # Configuración de la base de datos
    db_config = {
        'host': os.getenv("DB_HOST", "postgres"),
        'database': os.getenv("DB_NAME", "airflow"),
        'user': os.getenv("DB_USER", "airflow"),
        'password': os.getenv("DB_PASSWORD", "airflow"),
        'port': os.getenv("DB_PORT", "5432")
    }
    
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    
    # Cargar datos transformados
    products_df = pd.read_csv("data/transformed_products.csv")
    purchases_df = pd.read_csv("data/transformed_purchases.csv")
    purchase_products_df = pd.read_csv("data/transformed_purchase_products.csv")
    
    # Cargar a la base de datos
    products_df.to_sql('products', engine, if_exists='replace', index=False)
    purchases_df.to_sql('purchases', engine, if_exists='replace', index=False)
    purchase_products_df.to_sql('purchase_products', engine, if_exists='replace', index=False)
    
    print("Datos cargados exitosamente")
def main():
    load_to_postgres()
