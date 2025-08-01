import requests
import os
import json
import pandas as pd
import boto3


API_URL = "https://mnpwhdbcsk.us-east-2.awsapprunner.com/api"
API_KEY = "8yBO1wKiiIbcBT0"  # Key hardcodeada
os.makedirs("data", exist_ok=True)  # Esta línea es clave
def get_products():
    headers = {
        "x-api-key": API_KEY,
        "Accept": "application/json"
    }
    response = requests.get(f"{API_URL}/products", headers=headers)
    response.raise_for_status()
    return response.json()

def get_purchases():
    headers = {
        "x-api-key": API_KEY,
        "Accept": "application/json"
    }
    response = requests.get(f"{API_URL}/purchases", headers=headers)
    response.raise_for_status()
    return response.json()

def save_to_s3(data, filename, bucket_name="ecommerce-data-raw-dataengineer"):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )
    if filename == "products":
        folder = "products"
    elif filename == "purchases":
        folder = "purchases"
    else:
        folder = "otros"
    response = s3.put_object(
        Bucket=bucket_name,
        Key=f"{folder}/{filename}.json",
        Body=json.dumps(data)
    )
    print(f"[+] {filename}.json guardado en S3 - {response['ResponseMetadata']['HTTPStatusCode']}")


def main():
    products = get_products()
    purchases = get_purchases()

    os.makedirs("data", exist_ok=True)
    
    with open("data/products.json", "w") as f:
        json.dump(products, f)
    with open("data/purchases.json", "w") as f:
        json.dump(purchases, f)
    # Guardar en S3
    save_to_s3(products, "products")
    save_to_s3(purchases, "purchases")
    print("Datos extraídos y guardados correctamente.")
