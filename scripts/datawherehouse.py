# scripts/datawarehouse.py

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.abspath(os.path.join(script_dir, '..', 'docker', '.env'))
load_dotenv(dotenv_path)

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DW_DB_NAME = os.getenv('DW_POSTGRES_DB', 'AGORA_DATA_WHEREHOUSE')
DW_DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DW_DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

def connect_db(dbname):
    """Establishes a connection to the specified PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=dbname,
            user=DW_DB_USER,
            password=DW_DB_PASSWORD
        )
        conn.autocommit = True
        print(f"Connected to PostgreSQL database '{dbname}' successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL database '{dbname}': {e}")
        exit(1)

def create_database(conn, dbname):
    """Creates a new PostgreSQL database if it does not exist."""
    try:
        cur = conn.cursor()
        # Check if the database already exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;", (dbname,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"Database '{dbname}' created successfully.")
        else:
            print(f"Database '{dbname}' already exists. Skipping creation.")
        cur.close()
    except Exception as e:
        print(f"Error creating database '{dbname}': {e}")
        conn.rollback()
        cur.close()
        exit(1)

def drop_tables(conn):
    """Drop the custom tables from the database."""
    tables = [
        "FACT_SUPPLY_CHAIN",
        "FACT_TRANSACTIONS",
        "DIM_PRODUIT",
        "DIM_SUPPLIER",
        "DIM_TRANSPORT_MODE",
        "DIM_CLIENT",
        "DIM_TEMPS",
        "DIM_Time",
        "DIM_PAIEMENT",
        "DIM_SHOPPING_MALL"
    ]
    
    try:
        cur = conn.cursor()
        for table in tables:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
            print(f"Dropped table: {table}")
        cur.close()
        print("All specified tables dropped successfully.")
    except Exception as e:
        print(f"Error dropping tables: {e}")
        conn.rollback()
        if cur:
            cur.close()
        exit(1)

def create_tables(conn):
    """Creates the necessary tables in the data warehouse database."""
    commands = [
        """
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        """,
        # Dimension Tables
        """
        CREATE TABLE IF NOT EXISTS DIM_PRODUIT (
            SKU VARCHAR(255) PRIMARY KEY,
            Categorie VARCHAR(255),
            ProductName VARCHAR(255),
            Price FLOAT,
            Availability INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_SUPPLIER (
            SupplierID VARCHAR(255) PRIMARY KEY,
            SupplierName VARCHAR(255),
            SupplierCity VARCHAR(100),
            SupplierCountry VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_TRANSPORT_MODE (
            TransportModeID VARCHAR(255) PRIMARY KEY,
            TransportationModes VARCHAR(255),
            Routes VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_CLIENT (
            ClientID VARCHAR(255) PRIMARY KEY,
            LastName VARCHAR(100),
            FirstName VARCHAR(100),
            Gender VARCHAR(10),
            Age INTEGER,
            Address VARCHAR(255),
            City VARCHAR(100),
            CodePostal VARCHAR(20),
            Country VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_TEMPS (
            TransactionDate DATE PRIMARY KEY,
            Jour INTEGER,
            Mois INTEGER,
            Annee INTEGER,
            Quarter INTEGER,
            DayOfWeek INTEGER,
            IsWeekend BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_Time (
            DeliveryDate DATE PRIMARY KEY,
            Jour INTEGER,
            Mois INTEGER,
            Annee INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_PAIEMENT (
            PaiementID VARCHAR(255) PRIMARY KEY,
            MethodePaiement VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_SHOPPING_MALL (
            MallID VARCHAR(255) PRIMARY KEY,
            Name VARCHAR(255),
            Address VARCHAR(255),
            City VARCHAR(100),
            CodePostal VARCHAR(20),
            Country VARCHAR(100)
        );
        """,
        # Fact Tables
        """
        CREATE TABLE IF NOT EXISTS FACT_SUPPLY_CHAIN (
            FactID VARCHAR(255) PRIMARY KEY,
            SKU VARCHAR(255),
            SupplierID VARCHAR(255),
            TransportModeID VARCHAR(255),
            DeliveryDate DATE,
            StockLevel INTEGER,
            OrderQuantities INTEGER,
            ShippingCosts FLOAT,
            RevenueGenerated FLOAT,
            ManufacturingCosts FLOAT,
            DefectRates FLOAT,
            FOREIGN KEY (SKU) REFERENCES DIM_PRODUIT(SKU),
            FOREIGN KEY (SupplierID) REFERENCES DIM_SUPPLIER(SupplierID),
            FOREIGN KEY (TransportModeID) REFERENCES DIM_TRANSPORT_MODE(TransportModeID),
            FOREIGN KEY (DeliveryDate) REFERENCES DIM_Time(DeliveryDate)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
            FactID VARCHAR(255) PRIMARY KEY,
            TransactionID VARCHAR(255),
            ClientID VARCHAR(255),
            TransactionDate DATE,
            PaiementID VARCHAR(255),
            SKU VARCHAR(255),
            MallID VARCHAR(255),
            Quantite INTEGER,
            PrixAchat FLOAT,
            PrixVente FLOAT,
            Profit FLOAT,
            FOREIGN KEY (ClientID) REFERENCES DIM_CLIENT(ClientID),
            FOREIGN KEY (TransactionDate) REFERENCES DIM_TEMPS(TransactionDate),
            FOREIGN KEY (PaiementID) REFERENCES DIM_PAIEMENT(PaiementID),
            FOREIGN KEY (SKU) REFERENCES DIM_PRODUIT(SKU),
            FOREIGN KEY (MallID) REFERENCES DIM_SHOPPING_MALL(MallID)
        );
        """
    ]

    try:
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
            print("Executed command successfully.")
        cur.close()
        print("All tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
        if cur:
            cur.close()
        exit(1)

def main():
    # Step 1: Connect to the default 'postgres' database to create the 'AGORA_DATA_WHEREHOUSE' database
    conn_default = connect_db('postgres')
    create_database(conn_default, DW_DB_NAME)
    conn_default.close()

    # Step 2: Connect to the newly created 'AGORA_DATA_WHEREHOUSE' database to create tables
    conn_dw = connect_db(DW_DB_NAME)
    drop_tables(conn_dw) 
    create_tables(conn_dw)
    conn_dw.close()

if __name__ == '__main__':
    main()
