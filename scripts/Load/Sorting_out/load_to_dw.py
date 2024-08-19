import pandas as pd
from dotenv import load_dotenv
import os
import sys
sys.path.append('/opt/airflow/scripts')
from Load.db_connection import get_engine

# Carregar variáveis de ambiente
load_dotenv()

def detect_delimiter(file_path):
    with open(file_path, 'r') as file:
        first_line = file.readline()
        if ',' in first_line:
            return ','
        elif ';' in first_line:
            return ';'
        else:
            raise ValueError("Unknown delimiter")

def upload_to_sql(file_name=None):
    if file_name is None:
        print("Nenhum nome de arquivo fornecido.")
        return
    
    local_file_path = os.path.join('/opt/airflow/data_lake/Silver/Sorting_out', file_name)
    delimiter = detect_delimiter(local_file_path)
    
    # Carregar os dados do arquivo CSV
    try:
        df = pd.read_csv(local_file_path,delimiter=delimiter)
        print(f"Arquivo CSV '{file_name}' carregado com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar o arquivo CSV: {e}")
        return
    

    
    # Defina um mapeamento de índice para nome da coluna
    index_to_column_mapping = {
        0: 'warehouse',
        1: 'task_group_number',
        2: 'picking_task_number',
        3: 'picking_container',
        4: 'package_number',
        5: 'subpackage_number',
        6: 'basket_number',
        7: 'sorting_type',
        8: 'device_code',
        9: 'three_d_sorting_shelf',
        10: 'destination',
        11: 'mark_box_empty_name',
        12: 'operated_by',
        13: 'operation_time',
        14: 'sector',
        15: 'current_date_',
        16: 'extraction_hour',
        17: 'shift',
    }

    # Renomeie as colunas do DataFrame com base no mapeamento
    df.columns = [index_to_column_mapping.get(i) for i in range(len(df.columns))]
    
    # Insira os dados no banco de dados
    engine = get_engine()
    df.to_sql('sorting_out', engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso na tabela 'sorting_out'.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        upload_to_sql(file_name)
    else:
        print("Por favor, forneça o nome do arquivo CSV como argumento.")
