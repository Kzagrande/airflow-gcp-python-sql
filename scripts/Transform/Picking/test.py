import os
import pandas as pd
import sys
from datetime import datetime

def detect_delimiter(file_path):
    with open(file_path, 'r') as file:
        first_line = file.readline()
        if ',' in first_line:
            return ','
        elif ';' in first_line:
            return ';'
        else:
            raise ValueError("Unknown delimiter")

def extract_transform_print(file_name):
    print('FILE NAME:', file_name)
    file_path = os.path.join('/opt/airflow/data_lake/Bronze/Picking', file_name)
    
    try:
        # Detectar o delimitador
        delimiter = detect_delimiter(file_path)
        print(f"Detected delimiter: '{delimiter}'")
        
        df = pd.read_csv(file_path, delimiter=delimiter)

        df.iloc[:, 16].fillna(datetime(1500, 1, 11, 11, 11, 11), inplace=True)
        df.iloc[:, 17].fillna(datetime(1500, 1, 11, 11, 11, 11), inplace=True)
        df.iloc[:, 19].fillna(datetime(1500, 1, 11, 11, 11, 11), inplace=True)
        df.iloc[:, 21].fillna(datetime(1500, 1, 11, 11, 11, 11), inplace=True)

        df.iloc[:, 12] = pd.to_numeric(df.iloc[:, 12], errors='coerce').fillna(0).astype(int)

        df["sector"] = "picking"
        df["current_date_"] = datetime.now().strftime("%Y-%m-%d")
        df.fillna('-', inplace=True)

        hours = df.iloc[0, 19]  # completion time
        hours_date_type = datetime.strptime(hours, "%Y-%m-%d %H:%M:%S")
        print(type(hours_date_type))
        print(hours_date_type)
        hours_date_type = hours_date_type.replace(minute=0, second=0, microsecond=0)
        df["extraction_hour"] = hours_date_type

        # Salvar o DataFrame modificado
        output_path = os.path.join('/opt/airflow/data_lake/Silver/Picking', file_name)
        df.to_csv(output_path, index=False, sep=delimiter)

        print("DataFrame saved to:", output_path)
        print(df.head())
        
        # Apagar o arquivo original na Bronze
        os.remove(file_path)
        print(f"Original file {file_path} has been deleted.")
        
    except Exception as e:
        print(f"Error reading or processing file {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        extract_transform_print(file_name)
    else:
        print("File name argument is missing")
