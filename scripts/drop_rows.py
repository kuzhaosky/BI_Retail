# scripts/drop.py

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
DW_DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')

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

def truncate_tables(conn):
    """Truncates all tables in the database."""
    tables = [
        "fact_supply_chain",
        "fact_transactions",
        "dim_produit",
        "dim_supplier",
        "dim_transport_mode",
        "dim_client",
        "dim_temps",
        "dim_time",
        "dim_paiement",
        "dim_shopping_mall"
    ]

    try:
        cur = conn.cursor()
        # Disable foreign key checks
        cur.execute("SET session_replication_role = 'replica';")
        for table in tables:
            cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table)))
            print(f"Truncated table: {table}")
        # Re-enable foreign key checks
        cur.execute("SET session_replication_role = 'origin';")
        cur.close()
        print("All tables truncated successfully.")
    except Exception as e:
        print(f"Error truncating tables: {e}")
        conn.rollback()
        if cur:
            cur.close()
        exit(1)

def main():
    # Connect to the data warehouse database
    conn_dw = connect_db(DW_DB_NAME)
    truncate_tables(conn_dw)
    conn_dw.close()

if __name__ == '__main__':
    main()
