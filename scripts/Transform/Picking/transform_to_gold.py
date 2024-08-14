import os
import pandas as pd
import sys
from datetime import datetime
import numpy as np

def detect_delimiter(file_path):
    with open(file_path, 'r') as file:
        first_line = file.readline()
        if ',' in first_line:
            return ','
        elif ';' in first_line:
            return ';'
        else:
            raise ValueError("Unknown delimiter")

def transform_to_gold(file_name):
    print('FILE NAME:', file_name)
    file_path = os.path.join('/opt/airflow/data_lake/Silver/Picking', file_name)
    
    try:
        # Detectar o delimitador
        delimiter = detect_delimiter(file_path)
        print(f"Detected delimiter: '{delimiter}'")
        
        # Ler o arquivo CSV
        df = pd.read_csv(file_path, delimiter=delimiter)

        # Selecionar as colunas 0, 25, 23, e 18
        df = df.iloc[:, [0, 24, 23, 25, 8, 19, 18]]
        
        # Renomear as colunas para facilitar a referência
        df.columns = ['warehouse', 'current_date_', 'sector', 'extraction_hour', 'subpackage_number', 'picking_time', 'picker']

        df = df.sort_values(by=['picker', 'picking_time'])

        df['picking_time_next'] = df.groupby('picker')['picking_time'].shift(-1)
        df['picking_time_next'] = df['picking_time_next'].fillna(df['picking_time'])
        df['picking_time'] = pd.to_datetime(df['picking_time'])
        df['picking_time_next'] = pd.to_datetime(df['picking_time_next'])

        # Calcular a diferença em segundos entre 'picking_time' e 'picking_time_next'
        df['effective_hours'] = (df['picking_time_next'] - df['picking_time']).dt.total_seconds() 

        # Criar a coluna 'valido' com base na condição se 'effective_hours' <= 600
        df['valido'] = df['effective_hours'].apply(lambda x: 1 if x <= 600 else 0)

        # Agrupar por picker e calcular as quantidades
        grouped_df = df.groupby('picker').agg(
            warehouse=('warehouse', 'first'),
            current_date_=('current_date_', 'first'),
            sector=('sector', 'first'),
            extraction_hour=('extraction_hour', 'first'),
            effective_quantity=('subpackage_number', lambda x: (df.loc[x.index, 'valido'] == 1).sum()),
            real_quantity=('subpackage_number', 'count'),
            effective_hours=('effective_hours', lambda x: x[df['valido'] == 1].sum() / 3600)  # Convertendo segundos para horas
        ).reset_index()

        # Adicionar a coluna 'uph' diretamente no grouped_df
        grouped_df['uph'] = grouped_df.apply(
            lambda row: row['effective_quantity'] / row['effective_hours'] if row['effective_hours'] != 0 else np.nan,
            axis=1
        )
        
        # Formatar 'uph' para duas casas decimais
        grouped_df['uph'] = grouped_df['uph'].round(2)
        grouped_df['effective_hours'] = grouped_df['effective_hours'].round(2)

        # Adicionar a coluna 'work_occupation' calculando a diferença percentual
        grouped_df['work_occupation'] = (grouped_df['effective_hours'] / 1) 
        # Formatar 'work_occupation' para duas casas decimais
        grouped_df['work_occupation'] = grouped_df['effective_hours']

        print(df.head())
        print(grouped_df.head())

        # Salvar o DataFrame modificado (se necessário)
        output_path = os.path.join('/opt/airflow/data_lake/Gold/Picking', file_name)
        grouped_df.to_csv(output_path, index=False, sep=delimiter)
        print("DataFrame saved to:", output_path)
        
        # Apagar o arquivo original na Bronze (se necessário)
        os.remove(file_path)
        print(f"Original file {file_path} has been deleted.")
        
    except Exception as e:
        print(f"Error reading or processing file {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        transform_to_gold(file_name)
    else:
        print("File name argument is missing")
