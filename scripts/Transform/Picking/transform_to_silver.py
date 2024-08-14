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

def extract_transform_print(file_name, dw_file_path):
    print('FILE NAME:', file_name) #PICKING-2024-08-14-12.csv
    file_path = os.path.join(dw_file_path, file_name)
    
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
        return df

    except Exception as e:
        print(f"Error reading or processing file {file_path}: {e}")

def join_df(file_name, df_dwb, df_dwd):
    # Unir os dois DataFrames
    combined_df = pd.concat([df_dwb, df_dwd], ignore_index=True)

    # Detectar o delimitador para salvar o arquivo
    delimiter = ';'
    output_path = os.path.join('/opt/airflow/data_lake/Silver/Picking', file_name)
    
    # Salvar o DataFrame combinado
    combined_df.to_csv(output_path, index=False, sep=delimiter)

    print("Combined DataFrame saved to:", output_path)
    print(combined_df.head())
    
    # Apagar o arquivo original na Bronze
    os.remove(os.path.join('/opt/airflow/data_lake/Bronze/WHB/Picking', file_name))
    os.remove(os.path.join('/opt/airflow/data_lake/Bronze/WHD/Picking', file_name))
    print(f"Original files have been deleted.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        whb_filepath = '/opt/airflow/data_lake/Bronze/WHB/Picking'
        whd_filepath = '/opt/airflow/data_lake/Bronze/WHD/Picking'
        df_dwb = extract_transform_print(file_name, whb_filepath)
        df_dwd = extract_transform_print(file_name, whd_filepath)
        join_df(file_name, df_dwb, df_dwd)
    else:
        print("File name argument is missing")
