import pandas as pd

def load_data(file_path):
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.json'):
        df = pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file type. Please use CSV or JSON.")
    return df

# Example usage:
file_path = 'data.csv'  # or 'data.json'
data = load_data(file_path)
print(data.head())  # Show first few rows
