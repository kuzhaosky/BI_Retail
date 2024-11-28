import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime
import logging
import csv

# Initialize Faker
faker = Faker()

# Set up logging
logging.basicConfig(
    filename="supply_chain_generation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting supply chain data generation process...")

# Define constants
countries = {
    "France": ["Paris", "Lyon", "Marseille", "Lille", "Nice", "Toulouse"],
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco"],
    "England": ["London", "Manchester", "Birmingham", "Leeds", "Liverpool", "Bristol"]
}

delivery_companies = ["DHL", "FedEx", "UPS", "La Poste", "Royal Mail"]

cosmetics_mapping = {
    "Skincare": {"Products": ["Moisturizer", "Sunscreen", "Serum", "Face mask", "Cleanser", "Toner", "Exfoliator"], "BasePrice": 50},
    "Makeup": {"Products": ["Foundation", "Concealer", "Powder", "Lipstick", "Lip gloss", "Mascara", "Eyeshadow", "Eyeliner", "Blush", "Highlighter", "Brow pencil"], "BasePrice": 30},
    "Haircare": {"Products": ["Shampoo", "Conditioner", "Hair mask", "Hair oil", "Styling gel", "Hair spray", "Hair dye", "Heat protectant"], "BasePrice": 20},
    "Fragrances": {"Products": ["Perfume", "Body spray", "Deodorant", "Roll-on"], "BasePrice": 80},
    "Body Care": {"Products": ["Body lotion", "Body butter", "Shower gel", "Bath bomb", "Body scrub", "Hand cream", "Foot cream"], "BasePrice": 25},
    "Nail Care": {"Products": ["Nail polish", "Nail polish remover", "Nail treatment", "Nail tools"], "BasePrice": 15},
    "Men’s Grooming": {"Products": ["Shaving cream", "Beard oil", "Aftershave", "Face moisturizer for men", "Deodorant for men"], "BasePrice": 35},
    "Specialty Products": {"Products": ["Anti-aging cream", "Acne treatment", "Whitening cream", "Stretch mark cream", "Scar treatment"], "BasePrice": 60},
    "Organic or Natural Products": {"Products": ["Plant-based cream", "Vegan moisturizer", "Argan oil", "Jojoba oil", "Paraben-free shampoo"], "BasePrice": 40},
    "Baby Cosmetics": {"Products": ["Baby lotion", "Baby shampoo", "Baby oil", "Diaper cream", "Baby soap"], "BasePrice": 20},
    "Miscellaneous": {"Products": ["Makeup remover", "Setting spray", "Eyelash serum", "Cosmetic brushes", "Sponges"], "BasePrice": 10}
}

def calculate_unit_price(base_price, dates):
    """Calculates unit price based on inflation factor."""
    years_since_2021 = dates.year - 2021
    inflation_factors = 1 + (years_since_2021 * np.random.uniform(0.02, 0.08, size=len(dates)))
    return np.round(base_price * inflation_factors, 2)

# Generate and save data in batches
output_file = "supply_chain.csv"
batch_size = 100000
total_rows = 300000  

# Pre-generate Faker data
supplier_names = [faker.company() for _ in range(1000)]
supplier_cities = [faker.city() for _ in range(1000)]
supplier_countries = list(countries.keys())
store_countries = list(countries.keys())
delivery_companies_list = delivery_companies.copy()
product_categories = list(cosmetics_mapping.keys())
skus = [faker.uuid4() for _ in range(total_rows)]

# Pre-generate dates
start_date = datetime(2021, 1, 1)
end_date = datetime(2023, 12, 31)
delivery_dates = faker.date_between_dates(date_start=start_date, date_end=end_date)

# Set up CSV with header
with open(output_file, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=[
        "SupplierName", "SupplierCity", "SupplierCountry", "Category", "ProductName", 
        "SKU", "UnitPurchasePrice", "PurchasePrice", "DeliveryQuantity", "StoreName", 
        "StoreCity", "StoreCountry", "StockLevel", "DeliveryCompany", "DeliveryDate", 
        "OriginCity", "OriginCountry", "DestinationCity", "DestinationCountry", 
        "TransportCost"
    ])
    writer.writeheader()

    for batch_start in range(0, total_rows, batch_size):
        data_size = min(batch_size, total_rows - batch_start)
        try:
            # Random selections
            countries_array = np.random.choice(store_countries, size=data_size)
            cities_array = np.array([np.random.choice(countries[country]) for country in countries_array])
            categories_array = np.random.choice(product_categories, size=data_size)
            product_info = [cosmetics_mapping[cat] for cat in categories_array]
            product_names_array = np.array([np.random.choice(info["Products"]) for info in product_info])
            base_prices_array = np.array([info["BasePrice"] for info in product_info])
            delivery_dates_array = np.array([faker.date_between_dates(date_start=start_date, date_end=end_date) for _ in range(data_size)], dtype='datetime64[D]')
            unit_prices_array = calculate_unit_price(base_prices_array, pd.to_datetime(delivery_dates_array))
            delivery_quantities_array = np.random.randint(10, 501, size=data_size)
            purchase_prices_array = np.round(unit_prices_array * delivery_quantities_array, 2)
            transport_costs_array = np.round(np.random.uniform(50, 500, size=data_size), 2)
            supplier_names_array = np.random.choice(supplier_names, size=data_size)
            supplier_cities_array = np.random.choice(supplier_cities, size=data_size)
            supplier_countries_array = np.random.choice(supplier_countries, size=data_size)
            skus_array = skus[batch_start:batch_start+data_size]
            delivery_companies_array = np.random.choice(delivery_companies_list, size=data_size)
            origin_cities_array = np.random.choice(supplier_cities, size=data_size)
            origin_countries_array = np.random.choice(supplier_countries, size=data_size)

            # Prepare DataFrame
            df = pd.DataFrame({
                "SupplierName": supplier_names_array,
                "SupplierCity": supplier_cities_array,
                "SupplierCountry": supplier_countries_array,
                "Category": categories_array,
                "ProductName": product_names_array,
                "SKU": skus_array,
                "UnitPurchasePrice": unit_prices_array,
                "PurchasePrice": purchase_prices_array,
                "DeliveryQuantity": delivery_quantities_array,
                "StoreName": ["Agora " + city for city in cities_array],
                "StoreCity": cities_array,
                "StoreCountry": countries_array,
                "StockLevel": delivery_quantities_array,  # Initial stock equals delivery quantity
                "DeliveryCompany": delivery_companies_array,
                "DeliveryDate": delivery_dates_array.astype(str),
                "OriginCity": origin_cities_array,
                "OriginCountry": origin_countries_array,
                "DestinationCity": cities_array,
                "DestinationCountry": countries_array,
                "TransportCost": transport_costs_array
            })

            # Write the batch to CSV
            df.to_csv(file, header=False, index=False)
            logging.info(f"Wrote batch {batch_start + data_size}/{total_rows}.")
        except Exception as e:
            logging.error(f"Error generating batch starting at {batch_start}: {e}")

logging.info("Supply chain data generation process completed!")
