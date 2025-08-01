
import json
import pandas as pd
from datetime import datetime

def transform_products(products_data):
    """Transforma datos de productos (incluyendo anidamiento)"""
    df = pd.json_normalize(products_data, sep='_')
    print("🔍 Columnas originales:", df.columns.tolist())
    df = df.rename(columns={'createdAt': 'created_at'})
    df['created_at'] = pd.to_datetime(df['created_at']).dt.date
    df['price'] = df['price'].astype(float)
    return df[['id', 'name', 'description', 'price', 'category', 'created_at']]

def transform_purchases(purchases_data):
    """Transforma datos de compras"""
    df = pd.DataFrame(purchases_data)
    
    # Limpieza básica
    df['purchase_date'] = pd.to_datetime(df['purchaseDate']).dt.date
    df = df.rename(columns={'purchaseDate': 'purchase_date'})
    
    # Calcular total (opcional)
    if 'discount' in df.columns and 'subtotal' in df.columns:
        df['total'] = df['subtotal'] * (1 - df['discount']/100)
    
    return df[['id', 'status', 'creditCardType', 'purchase_date', 'total']]

def create_purchase_products(purchases_data):
    """Crea la tabla intermedia purchase_products"""
    records = []
    for purchase in purchases_data:
        for product in purchase['products']:
            records.append({
                'purchase_id': purchase['id'],
                'product_id': product['id'],
                'quantity': product['quantity']
            })
    return pd.DataFrame(records)

def main():
    with open("data/products.json") as f:
        products_data = json.load(f)
    with open("data/purchases.json") as f:
        purchases_data = json.load(f)

    products_df = transform_products(products_data)
    purchases_df = transform_purchases(purchases_data)
    purchase_products_df = create_purchase_products(purchases_data)

    products_df.to_csv("data/transformed_products.csv", index=False)
    purchases_df.to_csv("data/transformed_purchases.csv", index=False)
    purchase_products_df.to_csv("data/transformed_purchase_products.csv", index=False)

    print("Transformación completada y archivos guardados.")
