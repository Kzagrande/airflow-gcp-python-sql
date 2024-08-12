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

def transform_to_gold():
    file_name = "picking-2024-08-08-15.csv"
    print('FILE NAME:', file_name)
    file_path = os.path.join('/opt/airflow/data_lake/Silver/Picking', file_name)
    
    try:
        # Detectar o delimitador
        delimiter = detect_delimiter(file_path)
        print(f"Detected delimiter: '{delimiter}'")
        
        # Ler o arquivo CSV
        df = pd.read_csv(file_path, delimiter=delimiter)

        # Selecionar as colunas 0, 25, 23, e 18
        df = df.iloc[:, [0, 25, 23, 18, 8]]
        
        # Renomear as colunas para facilitar a referência
        df.columns = ['Transit warehouse', 'extraction_hour', 'sector', 'Picker', 'Sub-Package Number']
        
        # Fazer o group by na coluna 'Picker' e contar a coluna 'Sub-Package Number'
        df_grouped = df.groupby('Picker')['Sub-Package Number'].count().reset_index()
        
        # Renomear as colunas do DataFrame agrupado
        df_grouped.rename(columns={'Sub-Package Number': 'real_quantity'}, inplace=True)
        
        # Salvar o DataFrame modificado
        output_path = os.path.join('/opt/airflow/data_lake/Gold/Picking', file_name)
        df_grouped.to_csv(output_path, index=False, sep=delimiter)

        print("DataFrame saved to:", output_path)
        print(df_grouped.head())
        
        # Apagar o arquivo original na Bronze
        # os.remove(file_path)
        print(f"Original file {file_path} has been deleted.")
        
    except Exception as e:
        print(f"Error reading or processing file {file_path}: {e}")

if __name__ == "__main__":
    transform_to_gold()
    # if len(sys.argv) > 1:
    #     file_name = sys.argv[1]
    #     transform_to_gold(file_name)
    # else:
    #     print("File name argument is missing")
