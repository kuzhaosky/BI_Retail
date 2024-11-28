import pandas as pd
import random
from faker import Faker
from datetime import datetime
import logging
import csv

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

# Initialize list of SKUs with positive stock
positive_stock_skus = supply_chain_df.index[supply_chain_df["StockLevel"] > 0].tolist()

# Payment methods
payment_methods = ["Cash", "Credit Card", "Mobile Payment"]

profit_margin_percentage = 0.40

# Batch size and total rows
batch_size = 100000
total_rows = 500000
global_transaction_count = 0
logging.info(f"Target number of transactions: {total_rows}")

# Set up CSV with header
output_file = "transactions.csv"
with open(output_file, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=[
        "TransactionID", "ClientID", "ClientName", "StoreName", "StoreCity",
        "StoreCountry", "TransactionDate", "ProductName", "Category", "SKU",
        "UnitPurchasePrice", "UnitSellingPrice", "QuantityPurchased",
        "TotalPurchasePrice", "TotalSellingPrice", "Profit", "PaymentMethod"
    ])
    writer.writeheader()

    while global_transaction_count < total_rows and len(positive_stock_skus) > 0:
        batch_transactions = []
        for i in range(batch_size):
            if global_transaction_count >= total_rows or len(positive_stock_skus) == 0:
                break 

            try:
                sku = random.choice(positive_stock_skus)
                supply_row = supply_chain_df.loc[sku]

                # Determine quantity purchased
                max_quantity = supply_row["StockLevel"]
                quantity_purchased = random.randint(1, min(10, int(max_quantity)))

                unit_purchase_price = supply_row["UnitPurchasePrice"]

                # Calculate selling price with profit margin
                unit_selling_price = round(unit_purchase_price * (1 + profit_margin_percentage), 2)
                total_purchase_price = round(quantity_purchased * unit_purchase_price, 2)
                total_selling_price = round(quantity_purchased * unit_selling_price, 2)
                profit = round(total_selling_price - total_purchase_price, 2)

                # Generate transaction date
                delivery_date = pd.to_datetime(supply_row["DeliveryDate"])
                transaction_date = faker.date_between(
                    start_date=delivery_date,
                    end_date=datetime(2023, 12, 31)
                )

                transaction = {
                    "TransactionID": faker.uuid4(),
                    "ClientID": faker.uuid4(),
                    "ClientName": faker.name(),
                    "StoreName": supply_row["StoreName"],
                    "StoreCity": supply_row["StoreCity"],
                    "StoreCountry": supply_row["StoreCountry"],
                    "TransactionDate": transaction_date,
                    "ProductName": supply_row["ProductName"],
                    "Category": supply_row["Category"],
                    "SKU": sku,
                    "UnitPurchasePrice": unit_purchase_price,
                    "UnitSellingPrice": unit_selling_price,
                    "QuantityPurchased": quantity_purchased,
                    "TotalPurchasePrice": total_purchase_price,
                    "TotalSellingPrice": total_selling_price,
                    "Profit": profit,
                    "PaymentMethod": random.choice(payment_methods)
                }

                batch_transactions.append(transaction)

                # Update stock level
                supply_chain_df.loc[sku, "StockLevel"] -= quantity_purchased

                # Remove SKU from positive_stock_skus if stock is depleted
                if supply_chain_df.loc[sku, "StockLevel"] <= 0:
                    positive_stock_skus.remove(sku)

                # Increment global transaction count
                global_transaction_count += 1

                # Log progress every 10,000 transactions
                if (global_transaction_count % 10000) == 0:
                    logging.info(f"Generated {global_transaction_count}/{total_rows} transactions.")

            except Exception as e:
                logging.error(f"Error generating transaction: {e}")

        # Write the batch to CSV
        try:
            writer.writerows(batch_transactions)
            logging.info(f"Wrote batch of {len(batch_transactions)} transactions to '{output_file}'.")
        except Exception as e:
            logging.error(f"Error writing batch to '{output_file}': {e}")

# Save updated supply chain data back to CSV
try:
    supply_chain_df.reset_index().to_csv("supply_chain_updated.csv", index=False)
    logging.info("Updated supply chain data saved to 'supply_chain_updated.csv'.")
except Exception as e:
    logging.error(f"Error saving supply_chain_updated.csv: {e}")

logging.info("Transaction generation process completed!")
