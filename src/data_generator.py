import pandas as pd
from faker import Faker
from datetime import datetime
import os

fake = Faker()

def generate_sales_data(num_records=1000):
    """Generate fake sales data"""
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': fake.uuid4(),
            'customer_name': fake.name(),
            'product': fake.random_element(['Laptop', 'Phone', 'Tablet', 'Watch']),
            'price': round(fake.random_number(digits=4) / 10, 2),
            'quantity': fake.random_int(min=1, max=5),
            'transaction_date': fake.date_time_between(start_date='-30d', end_date='now'),
            'store_location': fake.city()
        })
    
    df = pd.DataFrame(data)
    
    # Save locally
    os.makedirs('/Volumes/omnidata/bucket/data/data_pipeline_demo/raw', exist_ok=True)
    filename = f"sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(f'/Volumes/omnidata/bucket/data/data_pipeline_demo/raw/{filename}', index=False)
    print(f"Generated {num_records} records in {filename}")
    return filename

if __name__ == "__main__":
    generate_sales_data()