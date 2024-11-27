import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.abspath(os.path.join(script_dir, '..', 'docker', '.env'))
load_dotenv(dotenv_path)

DB_HOST = 'localhost'
DB_PORT = '5433' 
DW_DB_NAME = os.getenv('DW_POSTGRES_DB', 'datawarehouse')
DW_DB_USER = os.getenv('POSTGRES_USER', 'dw_user')
DW_DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'dw_password')

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
        "DIM_SHOPING_MALL"
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
            SKU UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            Categorie VARCHAR(255),
            ProductName VARCHAR(255),
            Price FLOAT,
            Availability INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_SUPPLIER (
            SupplierID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            SupplierName VARCHAR(255),
            City VARCHAR(100),
            Country VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_TRANSPORT_MODE (
            TransportModeID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            TransportationModes VARCHAR(255),
            Routes VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_CLIENT (
            ClientID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
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
            DateTransaction DATE PRIMARY KEY,
            Jour INTEGER,
            Mois INTEGER,
            Annee INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_Time (
            deliveryDate DATE PRIMARY KEY,
            Jour INTEGER,
            Mois INTEGER,
            Annee INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_PAIEMENT (
            PaiementID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            MethodePaiement VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DIM_SHOPING_MALL (
            MallID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
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
            FactID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            SKU UUID REFERENCES DIM_PRODUIT(SKU),
            SupplierID UUID REFERENCES DIM_SUPPLIER(SupplierID),
            TransportModeID UUID REFERENCES DIM_TRANSPORT_MODE(TransportModeID),
            NumberOfProductsSold INTEGER,
            RevenueGenerated FLOAT,
            StockLevels INTEGER,
            OrderQuantities INTEGER,
            ShippingCosts FLOAT,
            ManufacturingCosts FLOAT,
            DefectRates FLOAT,
            deliveryDate DATE REFERENCES DIM_Time(deliveryDate)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
            TransactionID UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            ClientID UUID REFERENCES DIM_CLIENT(ClientID),
            DateTransaction DATE REFERENCES DIM_TEMPS(DateTransaction),
            PaiementID UUID REFERENCES DIM_PAIEMENT(PaiementID),
            ProduitID UUID REFERENCES DIM_PRODUIT(SKU),
            MallID UUID REFERENCES DIM_SHOPING_MALL(MallID),
            Quantite INTEGER,
            PrixTotal FLOAT
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
    # Step 1: Connect to the default 'postgres' database to create the 'datawarehouse' database
    conn_default = connect_db('postgres')
    create_database(conn_default, DW_DB_NAME)
    conn_default.close()

    # Step 2: Connect to the newly created 'datawarehouse' database to create tables
    conn_dw = connect_db(DW_DB_NAME)
    create_tables(conn_dw)
    conn_dw.close()

if __name__ == '__main__':
    main()
