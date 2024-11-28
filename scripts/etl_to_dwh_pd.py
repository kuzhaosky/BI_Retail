# scripts/etl_to_dwh.py

import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.abspath(os.path.join(script_dir, '..', 'docker', '.env'))
load_dotenv(dotenv_path)

DB_HOST = 'localhost'
DB_PORT = '5433' 
DW_DB_NAME = os.getenv('DW_POSTGRES_DB', 'datawarehouse')
DW_DB_USER = os.getenv('POSTGRES_USER', 'dw_user')
DW_DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'dw_password')

def get_database_engine():
    """Creates a SQLAlchemy engine for the PostgreSQL database."""
    try:
        engine = create_engine(f'postgresql://{DW_DB_USER}:{DW_DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DW_DB_NAME}')
        logger.info("Database engine created successfully.")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        exit(1)

def read_data(supply_chain_file, transactions_file):
    """Reads the CSV data into pandas DataFrames."""
    try:
        supply_chain = pd.read_csv(supply_chain_file)
        transactions = pd.read_csv(transactions_file)
        logger.info("Data read successfully from CSV files.")
        return supply_chain, transactions
    except Exception as e:
        logger.error(f"Error reading data from CSV files: {e}")
        exit(1)

def clean_data(supply_chain, transactions):
    """Performs data cleaning on the DataFrames."""
    # Remove duplicates
    supply_chain = supply_chain.drop_duplicates()
    transactions = transactions.drop_duplicates()

    # Handle missing values if any (assuming no missing values based on your tests)
    # But it's good practice to check and handle them
    supply_chain = supply_chain.dropna()
    transactions = transactions.dropna()

    # Ensure correct data types
    supply_chain['DeliveryDate'] = pd.to_datetime(supply_chain['DeliveryDate'])
    transactions['TransactionDate'] = pd.to_datetime(transactions['TransactionDate'])

    logger.info("Data cleaned successfully.")
    return supply_chain, transactions

def create_dim_produit(supply_chain):
    """Creates the DIM_PRODUIT dimension table."""
    dim_produit = supply_chain[['SKU', 'Category', 'ProductName', 'UnitPurchasePrice']].drop_duplicates()
    dim_produit = dim_produit.rename(columns={'UnitPurchasePrice': 'Price'})

    # Add Availability (StockLevel)
    stock_levels = supply_chain.groupby('SKU')['StockLevel'].sum().reset_index()
    dim_produit = pd.merge(dim_produit, stock_levels, on='SKU', how='left')
    dim_produit = dim_produit.rename(columns={'StockLevel': 'Availability'})

    logger.info("DIM_PRODUIT created successfully.")
    return dim_produit

def create_dim_supplier(supply_chain):
    """Creates the DIM_SUPPLIER dimension table."""
    dim_supplier = supply_chain[['SupplierName', 'SupplierCity', 'SupplierCountry']].drop_duplicates()
    # Create a unique SupplierID
    dim_supplier['SupplierID'] = [f"SUP-{i+1:05d}" for i in range(len(dim_supplier))]
    # Rearrange columns
    dim_supplier = dim_supplier[['SupplierID', 'SupplierName', 'SupplierCity', 'SupplierCountry']]

    logger.info("DIM_SUPPLIER created successfully.")
    return dim_supplier

def create_dim_transport_mode(supply_chain):
    """Creates the DIM_TRANSPORT_MODE dimension table."""
    dim_transport_mode = supply_chain[['DeliveryCompany']].drop_duplicates()
    dim_transport_mode['TransportModeID'] = [f"TRANS-{i+1:05d}" for i in range(len(dim_transport_mode))]
    dim_transport_mode['TransportationModes'] = dim_transport_mode['DeliveryCompany']
    # Placeholder for Routes
    dim_transport_mode['Routes'] = np.nan  # Replace with actual routes if available
    dim_transport_mode = dim_transport_mode[['TransportModeID', 'TransportationModes', 'Routes']]

    logger.info("DIM_TRANSPORT_MODE created successfully.")
    return dim_transport_mode

def create_dim_client(transactions):
    """Creates the DIM_CLIENT dimension table."""
    # Define countries and cities mapping (this could be moved outside if reused)
    countries = {
        "France": ["Paris", "Lyon", "Marseille", "Lille", "Nice", "Toulouse"],
        "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco"],
        "England": ["London", "Manchester", "Birmingham", "Leeds", "Liverpool", "Bristol"]
    }

    # Define postal code ranges per country
    postal_code_ranges = {
        "France": (75000, 75999), 
        "US": (10000, 99999),     
        "England": (1000, 9999)   
    }

    def generate_postal_code(country, city):
        if country in postal_code_ranges:
            start, end = postal_code_ranges[country]
            return np.random.randint(start, end + 1)
        else:
            return 99999

    # Extract unique clients
    unique_clients = transactions[['ClientID', 'ClientName', 'StoreCity', 'StoreCountry']].drop_duplicates()

    # Assign client locations based on store city and country
    unique_clients['City'] = unique_clients['StoreCity']
    unique_clients['Country'] = unique_clients['StoreCountry']

    # Generate gender
    np.random.seed(42)
    gender_choices = np.random.choice(['Female', 'Male'], size=len(unique_clients), p=[0.7, 0.3])
    unique_clients['Gender'] = gender_choices

    # Generate age distribution
    age_options = (
        list(range(16, 41)) * int(0.6 * len(unique_clients)) +  # 60% young
        list(range(41, 61)) * int(0.3 * len(unique_clients)) +  # 30% middle-aged
        list(range(61, 91)) * int(0.1 * len(unique_clients))    # 10% elderly
    )
    np.random.shuffle(age_options)
    unique_clients['Age'] = age_options[:len(unique_clients)]

    # Generate addresses
    unique_clients['Address'] = unique_clients['City'] + " Main Street"

    # Ensure consistency for duplicate clients
    final_dim_client = unique_clients.groupby('ClientID').first().reset_index()

    # Split FullName into FirstName and LastName
    final_dim_client[['FirstName', 'LastName']] = final_dim_client['ClientName'].str.split(' ', 1, expand=True)

    # Apply postal code generation
    final_dim_client['CodePostal'] = final_dim_client.apply(
        lambda row: generate_postal_code(row['Country'], row['City']),
        axis=1
    )

    # Rearrange columns
    final_dim_client = final_dim_client[['ClientID', 'FirstName', 'LastName', 'Gender', 'Age', 'Address', 'City', 'CodePostal', 'Country']]

    logger.info("DIM_CLIENT created successfully.")
    return final_dim_client

def create_dim_temps(transactions):
    """Creates the DIM_TEMPS dimension table."""
    dim_temps = transactions[['TransactionDate']].drop_duplicates()
    dim_temps['Jour'] = dim_temps['TransactionDate'].dt.day
    dim_temps['Mois'] = dim_temps['TransactionDate'].dt.month
    dim_temps['Annee'] = dim_temps['TransactionDate'].dt.year

    # Optionally add more time attributes
    dim_temps['Quarter'] = dim_temps['TransactionDate'].dt.quarter
    dim_temps['DayOfWeek'] = dim_temps['TransactionDate'].dt.dayofweek
    dim_temps['IsWeekend'] = dim_temps['DayOfWeek'] >= 5

    logger.info("DIM_TEMPS created successfully.")
    return dim_temps

def create_dim_paiement(transactions):
    """Creates the DIM_PAIEMENT dimension table."""
    dim_paiement = transactions[['PaymentMethod']].drop_duplicates()
    dim_paiement['PaiementID'] = [f"PAY-{i+1:05d}" for i in range(len(dim_paiement))]
    dim_paiement = dim_paiement[['PaiementID', 'PaymentMethod']]
    dim_paiement = dim_paiement.rename(columns={'PaymentMethod': 'MethodePaiement'})

    logger.info("DIM_PAIEMENT created successfully.")
    return dim_paiement

def create_dim_shopping_mall(transactions):
    """Creates the DIM_SHOPPING_MALL dimension table."""
    # Define postal code ranges per country (reuse from previous function or define globally)
    postal_code_ranges = {
        "France": (75000, 75999), 
        "US": (10000, 99999),     
        "England": (1000, 9999)   
    }

    def generate_postal_code(country, city):
        if country in postal_code_ranges:
            start, end = postal_code_ranges[country]
            return np.random.randint(start, end + 1)
        else:
            return 99999

    dim_shopping_mall = transactions[['StoreName', 'StoreCity', 'StoreCountry']].drop_duplicates()
    dim_shopping_mall['MallID'] = [f"MALL-{i+1:05d}" for i in range(len(dim_shopping_mall))]
    dim_shopping_mall['Address'] = dim_shopping_mall['StoreCity'] + " Mall Street"

    # Generate PostalCode
    dim_shopping_mall['CodePostal'] = dim_shopping_mall.apply(
        lambda row: generate_postal_code(row['StoreCountry'], row['StoreCity']),
        axis=1
    )

    # Rearrange columns and rename for consistency
    dim_shopping_mall = dim_shopping_mall.rename(columns={
        'StoreName': 'Name',
        'StoreCity': 'City',
        'StoreCountry': 'Country'
    })

    dim_shopping_mall = dim_shopping_mall[['MallID', 'Name', 'Address', 'City', 'CodePostal', 'Country']]

    logger.info("DIM_SHOPPING_MALL created successfully.")
    return dim_shopping_mall

def create_dim_time_delivery(supply_chain):
    """Creates the DIM_TIME dimension table for delivery dates."""
    dim_time_delivery = supply_chain[['DeliveryDate']].drop_duplicates()
    dim_time_delivery['Jour'] = dim_time_delivery['DeliveryDate'].dt.day
    dim_time_delivery['Mois'] = dim_time_delivery['DeliveryDate'].dt.month
    dim_time_delivery['Annee'] = dim_time_delivery['DeliveryDate'].dt.year

    # Optionally, create a surrogate key
    dim_time_delivery['DateID'] = dim_time_delivery['DeliveryDate'].dt.strftime('%Y%m%d').astype(int)

    # Rearrange columns
    dim_time_delivery = dim_time_delivery[['DeliveryDate', 'Jour', 'Mois', 'Annee']]

    logger.info("DIM_TIME_DELIVERY created successfully.")
    return dim_time_delivery

def create_fact_supply_chain(supply_chain, dim_supplier, dim_transport_mode, dim_produit, dim_time_delivery):
    """Creates the FACT_SUPPLY_CHAIN fact table."""
    # Merge necessary IDs
    fact_supply_chain = supply_chain.merge(dim_supplier, on=['SupplierName', 'SupplierCity', 'SupplierCountry'], how='left')
    fact_supply_chain = fact_supply_chain.merge(dim_transport_mode, left_on='DeliveryCompany', right_on='TransportationModes', how='left')
    fact_supply_chain = fact_supply_chain.merge(dim_produit[['SKU']], on='SKU', how='left')
    fact_supply_chain = fact_supply_chain.merge(dim_time_delivery[['DeliveryDate']], on='DeliveryDate', how='left')

    # Add measures and derived fields
    fact_supply_chain['RevenueGenerated'] = fact_supply_chain['PurchasePrice']
    fact_supply_chain['OrderQuantities'] = fact_supply_chain['DeliveryQuantity']
    fact_supply_chain['ShippingCosts'] = fact_supply_chain['TransportCost']
    fact_supply_chain['ManufacturingCosts'] = fact_supply_chain['UnitPurchasePrice'] * fact_supply_chain['DeliveryQuantity']
    fact_supply_chain['DefectRates'] = np.random.uniform(0, 0.05, len(fact_supply_chain))  # Placeholder for defect rates

    # Create FactID
    fact_supply_chain['FactID'] = [f"FACT-SC-{i+1:05d}" for i in range(len(fact_supply_chain))]

    # Select and rearrange columns
    fact_supply_chain = fact_supply_chain[['FactID', 'SKU', 'SupplierID', 'TransportModeID', 'DeliveryDate', 'StockLevel', 'OrderQuantities', 'ShippingCosts', 'RevenueGenerated', 'ManufacturingCosts', 'DefectRates']]

    logger.info("FACT_SUPPLY_CHAIN created successfully.")
    return fact_supply_chain

def create_fact_transactions(transactions, dim_client, dim_paiement, dim_shopping_mall, dim_produit, dim_temps):
    """Creates the FACT_TRANSACTIONS fact table."""
    # Merge necessary IDs
    fact_transactions = transactions.merge(dim_client, on='ClientID', how='left')
    fact_transactions = fact_transactions.merge(dim_paiement, left_on='PaymentMethod', right_on='MethodePaiement', how='left')
    fact_transactions = fact_transactions.merge(dim_shopping_mall, left_on=['StoreName', 'StoreCity', 'StoreCountry'], right_on=['Name', 'City', 'Country'], how='left')
    fact_transactions = fact_transactions.merge(dim_produit[['SKU']], on='SKU', how='left')
    fact_transactions = fact_transactions.merge(dim_temps[['TransactionDate']], on='TransactionDate', how='left')

    # Add unique FactID
    fact_transactions['FactID'] = [f"FACT-TR-{i+1:05d}" for i in range(len(fact_transactions))]

    # Rename columns
    fact_transactions = fact_transactions.rename(columns={
        'QuantityPurchased': 'Quantite',
        'TotalPurchasePrice': 'PrixAchat',
        'TotalSellingPrice': 'PrixVente'
    })

    # Select and rearrange columns
    fact_transactions = fact_transactions[['FactID', 'TransactionID', 'ClientID', 'TransactionDate', 'PaiementID', 'SKU', 'MallID', 'Quantite', 'PrixAchat', 'PrixVente', 'Profit']]

    logger.info("FACT_TRANSACTIONS created successfully.")
    return fact_transactions

def load_to_database(engine, dataframe, table_name):
    """Loads a DataFrame into the PostgreSQL database."""
    try:
        dataframe.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}")
        exit(1)

def main():
    # Step 1: Read data from CSV files
    supply_chain_file = 'supply_chain_updated.csv'
    transactions_file = 'transactions.csv'
    supply_chain, transactions = read_data(supply_chain_file, transactions_file)

    # Step 2: Clean data
    supply_chain, transactions = clean_data(supply_chain, transactions)

    # Step 3: Create dimension tables
    dim_produit = create_dim_produit(supply_chain)
    dim_supplier = create_dim_supplier(supply_chain)
    dim_transport_mode = create_dim_transport_mode(supply_chain)
    dim_client = create_dim_client(transactions)
    dim_temps = create_dim_temps(transactions)
    dim_paiement = create_dim_paiement(transactions)
    dim_shopping_mall = create_dim_shopping_mall(transactions)
    dim_time_delivery = create_dim_time_delivery(supply_chain)

    # Step 4: Create fact tables
    fact_supply_chain = create_fact_supply_chain(supply_chain, dim_supplier, dim_transport_mode, dim_produit, dim_time_delivery)
    fact_transactions = create_fact_transactions(transactions, dim_client, dim_paiement, dim_shopping_mall, dim_produit, dim_temps)

    # Step 5: Load data into the data warehouse
    engine = get_database_engine()

    # Load dimension tables
    load_to_database(engine, dim_produit, 'DIM_PRODUIT')
    load_to_database(engine, dim_supplier, 'DIM_SUPPLIER')
    load_to_database(engine, dim_transport_mode, 'DIM_TRANSPORT_MODE')
    load_to_database(engine, dim_client, 'DIM_CLIENT')
    load_to_database(engine, dim_temps, 'DIM_TEMPS')
    load_to_database(engine, dim_paiement, 'DIM_PAIEMENT')
    load_to_database(engine, dim_shopping_mall, 'DIM_SHOPING_MALL')
    load_to_database(engine, dim_time_delivery, 'DIM_Time')

    # Load fact tables
    load_to_database(engine, fact_supply_chain, 'FACT_SUPPLY_CHAIN')
    load_to_database(engine, fact_transactions, 'FACT_TRANSACTIONS')

    logger.info("ETL process completed successfully.")

if __name__ == '__main__':
    main()
