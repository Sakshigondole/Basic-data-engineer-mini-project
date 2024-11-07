import pandas as pd

# Create sample data
sample_data = {
    'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
    'product_id': [1, 2, 3, 4, 5],
    'quantity': [5, 3, 7, 2, 4],
    'unit_price': [10.99, 15.99, 8.99, 12.99, 9.99]
}

# Create DataFrame and save to CSV
df = pd.DataFrame(sample_data)
df.to_csv('sales_data.csv', index=False)
print("Sample CSV file created successfully!")