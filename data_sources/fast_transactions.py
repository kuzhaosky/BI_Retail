import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import logging
import uuid

faker = Faker()

# Set up logging
logging.basicConfig(
    filename="transactions_generation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting transaction generation process...")

# Load supply_chain.csv data
try:
    supply_chain_df = pd.read_csv("supply_chain.csv")
    logging.info("Loaded supply_chain.csv successfully.")
except FileNotFoundError as e:
    logging.error(f"File not found: {e}")
    exit(1)

# Set 'SKU' as the index for faster lookups
supply_chain_df.set_index('SKU', inplace=True)

# Payment methods
payment_methods = ["Cash", "Credit Card", "Mobile Payment"]

profit_margin_percentage = 0.40

transactions = []

logging.info("Generating transactions per SKU...")

# Function to generate transaction quantities for each SKU
def generate_quantities(stock_level):
    quantities = []
    total_quantity_sold = 0
    while total_quantity_sold < stock_level:
        max_possible_quantity = min(10, stock_level - total_quantity_sold)
        quantity = np.random.randint(1, max_possible_quantity)
        quantities.append(quantity)
        total_quantity_sold += quantity
    return quantities

# Generate transactions per SKU
for sku, supply_row in supply_chain_df.iterrows():
    stock_level = int(supply_row["StockLevel"])
    if stock_level <= 0:
        continue  # Skip SKUs with no stock

    quantities = generate_quantities(stock_level)
    for quantity in quantities:
        transactions.append({
            "SKU": sku,
            "QuantityPurchased": quantity,
            "DeliveryDate": supply_row["DeliveryDate"],
            "UnitPurchasePrice": supply_row["UnitPurchasePrice"],
            "StoreName": supply_row["StoreName"],
            "StoreCity": supply_row["StoreCity"],
            "StoreCountry": supply_row["StoreCountry"],
            "ProductName": supply_row["ProductName"],
            "Category": supply_row["Category"]
        })

logging.info("Transactions generated per SKU.")

# Create DataFrame
transactions_df = pd.DataFrame(transactions)

# Compute unit_selling_price, total_purchase_price, total_selling_price, profit
transactions_df["UnitSellingPrice"] = np.round(
    transactions_df["UnitPurchasePrice"] * (1 + profit_margin_percentage), 2)
transactions_df["TotalPurchasePrice"] = np.round(
    transactions_df["UnitPurchasePrice"] * transactions_df["QuantityPurchased"], 2)
transactions_df["TotalSellingPrice"] = np.round(
    transactions_df["UnitSellingPrice"] * transactions_df["QuantityPurchased"], 2)
transactions_df["Profit"] = transactions_df["TotalSellingPrice"] - transactions_df["TotalPurchasePrice"]

# Generate a list of unique ClientIDs and ClientNames
num_clients = 10000  # Number of unique clients
client_ids = [str(uuid.uuid4()) for _ in range(num_clients)]
client_names = [faker.name() for _ in range(num_clients)]

num_transactions = len(transactions_df)

transactions_df["ClientID"] = np.random.choice(client_ids, size=num_transactions)
transactions_df["ClientName"] = np.random.choice(client_names, size=num_transactions)

# Generate TransactionID
transactions_df["TransactionID"] = ["T{:09d}".format(i) for i in range(1, num_transactions + 1)]

# Generate PaymentMethod
transactions_df["PaymentMethod"] = np.random.choice(payment_methods, size=num_transactions)

# Generate TransactionDate between DeliveryDate and 2023-12-31
delivery_dates = pd.to_datetime(transactions_df["DeliveryDate"])
end_date = pd.to_datetime("2023-12-31")
max_days = (end_date - delivery_dates).dt.days

# Generate random days offset
np.random.seed(0)  # For reproducibility
random_days = np.array([np.random.randint(0, max_day + 1) if max_day > 0 else 0 for max_day in max_days])

transactions_df["TransactionDate"] = delivery_dates + pd.to_timedelta(random_days, unit='D')
transactions_df["TransactionDate"] = transactions_df["TransactionDate"].dt.date

# Select required columns
output_columns = [
    "TransactionID", "ClientID", "ClientName", "StoreName", "StoreCity",
    "StoreCountry", "TransactionDate", "ProductName", "Category", "SKU",
    "UnitPurchasePrice", "UnitSellingPrice", "QuantityPurchased",
    "TotalPurchasePrice", "TotalSellingPrice", "Profit", "PaymentMethod"
]

transactions_df = transactions_df[output_columns]

# Limit to the desired total number of transactions
total_rows = 500000
if len(transactions_df) > total_rows:
    transactions_df = transactions_df.sample(n=total_rows)
    logging.info(f"Sampled down to {total_rows} transactions.")

# Write to CSV
transactions_df.to_csv("transactions.csv", index=False)
logging.info("Transactions saved to 'transactions.csv'.")

# Update stock levels
quantities_sold_per_sku = transactions_df.groupby("SKU")["QuantityPurchased"].sum()
supply_chain_df["StockLevel"] -= quantities_sold_per_sku

# Save updated supply chain data back to CSV
supply_chain_df.reset_index().to_csv("supply_chain_updated.csv", index=False)
logging.info("Updated supply chain data saved to 'supply_chain_updated.csv'.")

logging.info("Transaction generation process completed!")
