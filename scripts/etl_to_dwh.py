import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, udf, when, concat_ws, trim, lower,
    row_number, split, dayofmonth, month, year, quarter, dayofweek
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.abspath(os.path.join(script_dir, '..', 'docker', '.env'))
load_dotenv(dotenv_path)

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DW_DB_NAME = os.getenv('DW_POSTGRES_DB', 'AGORA_DATA_WHEREHOUSE')
DW_DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DW_DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

def get_spark_session():
    """Creates a SparkSession."""
    try:
        spark = SparkSession.builder \
            .appName("ETL_to_DWH") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
            .getOrCreate()
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        exit(1)

def read_data(spark, supply_chain_file, transactions_file):
    """Reads the CSV data into Spark DataFrames."""
    try:
        supply_chain = spark.read.csv(supply_chain_file, header=True, inferSchema=True)
        transactions = spark.read.csv(transactions_file, header=True, inferSchema=True)
        logger.info("Data read successfully from CSV files.")
        return supply_chain, transactions
    except Exception as e:
        logger.error(f"Error reading data from CSV files: {e}")
        exit(1)

def clean_data(supply_chain, transactions):
    """Performs data cleaning on the DataFrames."""
    # Remove duplicates
    supply_chain = supply_chain.dropDuplicates()
    transactions = transactions.dropDuplicates()

    # Handle missing values if any
    supply_chain = supply_chain.na.drop()
    transactions = transactions.na.drop()

    # Ensure correct data types
    supply_chain = supply_chain.withColumn('DeliveryDate', col('DeliveryDate').cast(DateType()))
    transactions = transactions.withColumn('TransactionDate', col('TransactionDate').cast(DateType()))

    logger.info("Data cleaned successfully.")
    return supply_chain, transactions

def create_dim_produit(supply_chain):
    """Creates the DIM_PRODUIT dimension table."""
    dim_produit = supply_chain.select('SKU', 'Category', 'ProductName', 'UnitPurchasePrice').dropDuplicates()

    # Rename columns to match database schema (all lowercase)
    dim_produit = dim_produit.withColumnRenamed('SKU', 'sku') \
                             .withColumnRenamed('Category', 'categorie') \
                             .withColumnRenamed('ProductName', 'productname') \
                             .withColumnRenamed('UnitPurchasePrice', 'price')

    # Add Availability (StockLevel)
    stock_levels = supply_chain.groupBy('SKU').sum('StockLevel').withColumnRenamed('sum(StockLevel)', 'availability')
    stock_levels = stock_levels.withColumnRenamed('SKU', 'sku')
    dim_produit = dim_produit.join(stock_levels, on='sku', how='left')

    logger.info("DIM_PRODUIT created successfully.")
    return dim_produit

def create_dim_supplier(supply_chain):
    """Creates the DIM_SUPPLIER dimension table."""
    dim_supplier = supply_chain.select('SupplierName', 'SupplierCity', 'SupplierCountry').dropDuplicates()

    # Rename and standardize columns
    dim_supplier = dim_supplier.withColumnRenamed('SupplierName', 'suppliername') \
                               .withColumnRenamed('SupplierCity', 'suppliercity') \
                               .withColumnRenamed('SupplierCountry', 'suppliercountry') \
                               .withColumn('suppliername', trim(lower(col('suppliername')))) \
                               .withColumn('suppliercity', trim(lower(col('suppliercity')))) \
                               .withColumn('suppliercountry', trim(lower(col('suppliercountry'))))

    # Create a unique SupplierID using row_number with partitioning
    window_spec = Window.partitionBy('suppliercountry').orderBy('suppliername', 'suppliercity')
    dim_supplier = dim_supplier.withColumn('supplierid_seq', row_number().over(window_spec))
    dim_supplier = dim_supplier.withColumn('supplierid', concat_ws('-', lit('SUP'), col('supplierid_seq'), col('suppliercountry')))

    # Rearrange columns
    dim_supplier = dim_supplier.select('supplierid', 'suppliername', 'suppliercity', 'suppliercountry')

    logger.info("DIM_SUPPLIER created successfully.")
    return dim_supplier

def create_dim_transport_mode(supply_chain):
    """Creates the DIM_TRANSPORT_MODE dimension table."""
    dim_transport_mode = supply_chain.select('DeliveryCompany').dropDuplicates()

    # Rename and standardize columns
    dim_transport_mode = dim_transport_mode.withColumnRenamed('DeliveryCompany', 'transportationmodes') \
                                           .withColumn('transportationmodes', trim(lower(col('transportationmodes'))))

    # Create TransportModeID using row_number with partitioning
    window_spec = Window.orderBy('transportationmodes')
    dim_transport_mode = dim_transport_mode.withColumn('transportmodeid_seq', row_number().over(window_spec))
    dim_transport_mode = dim_transport_mode.withColumn('transportmodeid', concat_ws('-', lit('TRANS'), col('transportmodeid_seq')))

    # Placeholder for Routes
    dim_transport_mode = dim_transport_mode.withColumn('routes', lit(None).cast(StringType()))

    # Rearrange columns
    dim_transport_mode = dim_transport_mode.select('transportmodeid', 'transportationmodes', 'routes')

    logger.info("DIM_TRANSPORT_MODE created successfully.")
    return dim_transport_mode

def create_dim_client(transactions):
    """Creates the DIM_CLIENT dimension table."""
    # Define UDFs for postal code generation
    postal_code_ranges = {
        "france": (75000, 75999),
        "us": (10000, 99999),
        "england": (1000, 9999)
    }

    def generate_postal_code(country):
        country = country.lower()
        if country in postal_code_ranges:
            start, end = postal_code_ranges[country]
            return int(np.random.randint(start, end + 1))
        else:
            return 99999

    udf_postal_code = udf(generate_postal_code, IntegerType())

    # Extract unique clients
    unique_clients = transactions.select('ClientID', 'ClientName', 'StoreCity', 'StoreCountry').dropDuplicates()

    # Rename and standardize columns
    unique_clients = unique_clients.withColumnRenamed('ClientID', 'clientid') \
                                   .withColumnRenamed('ClientName', 'clientname') \
                                   .withColumnRenamed('StoreCity', 'city') \
                                   .withColumnRenamed('StoreCountry', 'country') \
                                   .withColumn('city', trim(lower(col('city')))) \
                                   .withColumn('country', trim(lower(col('country'))))

    # Generate gender using row_number with partitioning
    window_spec_gender = Window.partitionBy('country').orderBy('clientid')
    unique_clients = unique_clients.withColumn('row_num', row_number().over(window_spec_gender))
    unique_clients = unique_clients.withColumn(
        'gender',
        when((col('row_num') % 10) < 7, lit('Female')).otherwise(lit('Male'))
    ).drop('row_num')

    # Generate age distribution
    window_spec_age = Window.partitionBy('country').orderBy('clientid')
    unique_clients = unique_clients.withColumn('row_num', row_number().over(window_spec_age))
    unique_clients = unique_clients.withColumn(
        'age',
        when((col('row_num') % 10) < 6, lit(16) + (col('row_num') % 25)) \
        .when((col('row_num') % 10) < 9, lit(41) + (col('row_num') % 20)) \
        .otherwise(lit(61) + (col('row_num') % 30))
    ).drop('row_num')

    # Generate addresses
    unique_clients = unique_clients.withColumn('address', concat_ws(' ', col('city'), lit('Main Street')))

    # Split FullName into FirstName and LastName
    split_col = split(unique_clients['clientname'], ' ')
    unique_clients = unique_clients.withColumn('firstname', split_col.getItem(0))
    unique_clients = unique_clients.withColumn('lastname', split_col.getItem(1))

    # Apply postal code generation
    unique_clients = unique_clients.withColumn('codepostal', udf_postal_code(col('country')))

    # Rearrange columns
    final_dim_client = unique_clients.select('clientid', 'lastname', 'firstname', 'gender', 'age', 'address', 'city', 'codepostal', 'country')

    logger.info("DIM_CLIENT created successfully.")
    return final_dim_client

def create_dim_temps(transactions):
    """Creates the DIM_TEMPS dimension table."""
    dim_temps = transactions.select('TransactionDate').dropDuplicates()
    dim_temps = dim_temps.withColumnRenamed('TransactionDate', 'transactiondate')

    dim_temps = dim_temps.withColumn('jour', dayofmonth('transactiondate')) \
                         .withColumn('mois', month('transactiondate')) \
                         .withColumn('annee', year('transactiondate')) \
                         .withColumn('quarter', quarter('transactiondate')) \
                         .withColumn('dayofweek', dayofweek('transactiondate')) \
                         .withColumn('isweekend', when(col('dayofweek').isin([1, 7]), True).otherwise(False))

    logger.info("DIM_TEMPS created successfully.")
    return dim_temps

def create_dim_paiement(transactions):
    """Creates the DIM_PAIEMENT dimension table."""
    dim_paiement = transactions.select('PaymentMethod').dropDuplicates()
    dim_paiement = dim_paiement.withColumn('methodepaiement', trim(lower(col('PaymentMethod'))))

    # Create PaiementID using row_number with partitioning
    window_spec = Window.orderBy('methodepaiement')
    dim_paiement = dim_paiement.withColumn('paiementid_seq', row_number().over(window_spec))
    dim_paiement = dim_paiement.withColumn('paiementid', concat_ws('-', lit('PAY'), col('paiementid_seq')))

    # Rearrange columns
    dim_paiement = dim_paiement.select('paiementid', 'methodepaiement')

    logger.info("DIM_PAIEMENT created successfully.")
    return dim_paiement

def create_dim_shopping_mall(transactions):
    """Creates the DIM_SHOPPING_MALL dimension table."""
    # Define UDFs for postal code generation
    postal_code_ranges = {
        "france": (75000, 75999),
        "us": (10000, 99999),
        "england": (1000, 9999)
    }

    def generate_postal_code(country):
        country = country.lower()
        if country in postal_code_ranges:
            start, end = postal_code_ranges[country]
            return int(np.random.randint(start, end + 1))
        else:
            return 99999

    udf_postal_code = udf(generate_postal_code, IntegerType())

    # Select unique malls
    dim_shopping_mall = transactions.select('StoreName', 'StoreCity', 'StoreCountry').dropDuplicates()

    # Rename and standardize columns
    dim_shopping_mall = dim_shopping_mall.withColumnRenamed('StoreName', 'name') \
                                         .withColumnRenamed('StoreCity', 'city') \
                                         .withColumnRenamed('StoreCountry', 'country') \
                                         .withColumn('name', trim(lower(col('name')))) \
                                         .withColumn('city', trim(lower(col('city')))) \
                                         .withColumn('country', trim(lower(col('country'))))

    # Add MallID using row_number with partitioning
    window_spec = Window.partitionBy('country').orderBy('name', 'city')
    dim_shopping_mall = dim_shopping_mall.withColumn('mallid_seq', row_number().over(window_spec))
    dim_shopping_mall = dim_shopping_mall.withColumn('mallid', concat_ws('-', lit('MALL'), col('mallid_seq'), col('country')))

    # Add Address
    dim_shopping_mall = dim_shopping_mall.withColumn('address', concat_ws(' ', col('city'), lit('Mall Street')))

    # Apply postal code generation
    dim_shopping_mall = dim_shopping_mall.withColumn('codepostal', udf_postal_code(col('country')))

    # Rearrange columns
    dim_shopping_mall = dim_shopping_mall.select('mallid', 'name', 'address', 'city', 'codepostal', 'country')

    logger.info("DIM_SHOPPING_MALL created successfully.")
    return dim_shopping_mall

def create_dim_time_delivery(supply_chain):
    """Creates the DIM_Time dimension table for delivery dates."""
    dim_time_delivery = supply_chain.select('DeliveryDate').dropDuplicates()
    dim_time_delivery = dim_time_delivery.withColumnRenamed('DeliveryDate', 'deliverydate')

    dim_time_delivery = dim_time_delivery.withColumn('jour', dayofmonth('deliverydate')) \
                                         .withColumn('mois', month('deliverydate')) \
                                         .withColumn('annee', year('deliverydate'))

    logger.info("DIM_Time created successfully.")
    return dim_time_delivery

def create_fact_supply_chain(supply_chain, dim_supplier, dim_transport_mode):
    """Creates the FACT_SUPPLY_CHAIN fact table."""
    # Rename and standardize columns in supply_chain
    supply_chain = supply_chain.withColumnRenamed('SKU', 'sku') \
                               .withColumnRenamed('SupplierName', 'suppliername') \
                               .withColumnRenamed('SupplierCity', 'suppliercity') \
                               .withColumnRenamed('SupplierCountry', 'suppliercountry') \
                               .withColumnRenamed('DeliveryCompany', 'deliverycompany') \
                               .withColumnRenamed('DeliveryDate', 'deliverydate') \
                               .withColumnRenamed('StockLevel', 'stocklevel') \
                               .withColumn('suppliername', trim(lower(col('suppliername')))) \
                               .withColumn('suppliercity', trim(lower(col('suppliercity')))) \
                               .withColumn('suppliercountry', trim(lower(col('suppliercountry')))) \
                               .withColumn('deliverycompany', trim(lower(col('deliverycompany'))))

    # Join with SupplierID
    fact_supply_chain = supply_chain.join(dim_supplier, on=['suppliername', 'suppliercity', 'suppliercountry'], how='left')

    # Join with TransportModeID
    fact_supply_chain = fact_supply_chain.join(dim_transport_mode, fact_supply_chain['deliverycompany'] == dim_transport_mode['transportationmodes'], how='left')

    # Check for missing supplierids
    missing_supplierid_count = fact_supply_chain.filter(col('supplierid').isNull()).count()
    if missing_supplierid_count > 0:
        logger.warning(f"There are {missing_supplierid_count} records in FACT_SUPPLY_CHAIN with missing supplierid.")
    else:
        logger.info("All supplierids in FACT_SUPPLY_CHAIN are present in DIM_SUPPLIER.")

    # Add measures and derived fields
    fact_supply_chain = fact_supply_chain.withColumn('revenuegenerated', col('PurchasePrice')) \
                                         .withColumn('orderquantities', col('DeliveryQuantity')) \
                                         .withColumn('shippingcosts', col('TransportCost')) \
                                         .withColumn('manufacturingcosts', col('UnitPurchasePrice') * col('DeliveryQuantity')) \
                                         .withColumn('defectrates', lit(np.random.uniform(0, 0.05)))  # Placeholder

    # Create FactID using row_number with partitioning
    window_spec = Window.partitionBy('supplierid').orderBy('sku', 'deliverydate')
    fact_supply_chain = fact_supply_chain.withColumn('factid_seq', row_number().over(window_spec))
    fact_supply_chain = fact_supply_chain.withColumn('factid', concat_ws('-', lit('FACT-SC'), col('factid_seq'), col('supplierid')))

    # Select and rearrange columns
    fact_supply_chain = fact_supply_chain.select(
        'factid', 'sku', 'supplierid', 'transportmodeid',
        'deliverydate', 'stocklevel', 'orderquantities',
        'shippingcosts', 'revenuegenerated', 'manufacturingcosts', 'defectrates'
    )

    logger.info("FACT_SUPPLY_CHAIN created successfully.")
    return fact_supply_chain

def create_fact_transactions(transactions, dim_client, dim_paiement, dim_shopping_mall):
    """Creates the FACT_TRANSACTIONS fact table."""
    # Rename and standardize columns in transactions
    transactions = transactions.withColumnRenamed('TransactionID', 'transactionid') \
                               .withColumnRenamed('ClientID', 'clientid') \
                               .withColumnRenamed('TransactionDate', 'transactiondate') \
                               .withColumnRenamed('PaymentMethod', 'paymentmethod') \
                               .withColumnRenamed('StoreName', 'storename') \
                               .withColumnRenamed('StoreCity', 'storecity') \
                               .withColumnRenamed('StoreCountry', 'storecountry') \
                               .withColumnRenamed('QuantityPurchased', 'quantite') \
                               .withColumnRenamed('TotalPurchasePrice', 'prixachat') \
                               .withColumnRenamed('TotalSellingPrice', 'prixvente') \
                               .withColumnRenamed('Profit', 'profit') \
                               .withColumnRenamed('SKU', 'sku') \
                               .withColumn('paymentmethod', trim(lower(col('paymentmethod')))) \
                               .withColumn('storename', trim(lower(col('storename')))) \
                               .withColumn('storecity', trim(lower(col('storecity')))) \
                               .withColumn('storecountry', trim(lower(col('storecountry'))))

    # Join with DIM_CLIENT
    fact_transactions = transactions.join(dim_client, on='clientid', how='left')

    # Join with DIM_PAIEMENT
    fact_transactions = fact_transactions.join(dim_paiement, transactions['paymentmethod'] == dim_paiement['methodepaiement'], how='left')

    # Join with DIM_SHOPPING_MALL
    fact_transactions = fact_transactions.join(dim_shopping_mall,
                                               (transactions['storename'] == dim_shopping_mall['name']) &
                                               (transactions['storecity'] == dim_shopping_mall['city']) &
                                               (transactions['storecountry'] == dim_shopping_mall['country']),
                                               how='left')

    # Check for missing IDs
    missing_paiementid_count = fact_transactions.filter(col('paiementid').isNull()).count()
    missing_mallid_count = fact_transactions.filter(col('mallid').isNull()).count()

    if missing_paiementid_count > 0:
        logger.warning(f"There are {missing_paiementid_count} records in FACT_TRANSACTIONS with missing paiementid.")
    if missing_mallid_count > 0:
        logger.warning(f"There are {missing_mallid_count} records in FACT_TRANSACTIONS with missing mallid.")

    # Create FactID using row_number with partitioning
    window_spec = Window.partitionBy('clientid').orderBy('transactiondate', 'transactionid')
    fact_transactions = fact_transactions.withColumn('factid_seq', row_number().over(window_spec))
    fact_transactions = fact_transactions.withColumn('factid', concat_ws('-', lit('FACT-TR'), col('factid_seq'), col('clientid')))

    # Select and rearrange columns
    fact_transactions = fact_transactions.select(
        'factid', 'transactionid', 'clientid', 'transactiondate',
        'paiementid', 'sku', 'mallid', 'quantite', 'prixachat',
        'prixvente', 'profit'
    )

    logger.info("FACT_TRANSACTIONS created successfully.")
    return fact_transactions

def write_to_postgres(df, table_name):
    """Writes a DataFrame to PostgreSQL using JDBC."""
    url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DW_DB_NAME}"
    properties = {
        "user": DW_DB_USER,
        "password": DW_DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # Convert DataFrame column names to lowercase
    df = df.toDF(*[c.lower() for c in df.columns])
    table_name = table_name.lower()

    try:
        df.write.jdbc(url=url, table=table_name, mode='append', properties=properties)
        logger.info(f"Data written to table {table_name} successfully.")
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL table {table_name}: {e}")
        exit(1)

def main():
    # Step 1: Create SparkSession
    spark = get_spark_session()

    # Step 2: Read data from CSV files
    supply_chain_file = os.path.abspath(os.path.join(script_dir, '..', 'data_sources', 'data', 'supply_chain_updated.csv'))
    transactions_file = os.path.abspath(os.path.join(script_dir, '..', 'data_sources', 'data', 'transactions.csv'))
    supply_chain, transactions = read_data(spark, supply_chain_file, transactions_file)

    # Step 3: Clean data
    supply_chain, transactions = clean_data(supply_chain, transactions)

    # Step 4: Create dimension tables
    dim_produit = create_dim_produit(supply_chain)
    dim_supplier = create_dim_supplier(supply_chain)
    dim_transport_mode = create_dim_transport_mode(supply_chain)
    dim_client = create_dim_client(transactions)
    dim_temps = create_dim_temps(transactions)
    dim_paiement = create_dim_paiement(transactions)
    dim_shopping_mall = create_dim_shopping_mall(transactions)
    dim_time_delivery = create_dim_time_delivery(supply_chain)

    # Step 5: Create fact tables
    fact_supply_chain = create_fact_supply_chain(supply_chain, dim_supplier, dim_transport_mode)
    fact_transactions = create_fact_transactions(transactions, dim_client, dim_paiement, dim_shopping_mall)

    # Step 6: Write data to PostgreSQL
    write_to_postgres(dim_produit, 'DIM_PRODUIT')
    write_to_postgres(dim_supplier, 'DIM_SUPPLIER')
    write_to_postgres(dim_transport_mode, 'DIM_TRANSPORT_MODE')
    write_to_postgres(dim_client, 'DIM_CLIENT')
    write_to_postgres(dim_temps, 'DIM_TEMPS')
    write_to_postgres(dim_paiement, 'DIM_PAIEMENT')
    write_to_postgres(dim_shopping_mall, 'DIM_SHOPPING_MALL')
    write_to_postgres(dim_time_delivery, 'DIM_Time')

    write_to_postgres(fact_supply_chain, 'FACT_SUPPLY_CHAIN')
    write_to_postgres(fact_transactions, 'FACT_TRANSACTIONS')

    logger.info("ETL process completed successfully.")

    # Stop SparkSession
    spark.stop()

if __name__ == '__main__':
    main()
