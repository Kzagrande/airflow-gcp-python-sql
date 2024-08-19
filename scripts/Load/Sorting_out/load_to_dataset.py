import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import sys

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
    
    local_file_path = os.path.join('/opt/airflow/data_lake/Silver/Putaway', file_name)
    delimiter = detect_delimiter(local_file_path)
    
    # Carregar os dados do arquivo CSV
    try:
        df = pd.read_csv(local_file_path,delimiter=delimiter)
        print(f"Arquivo CSV '{file_name}' carregado com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar o arquivo CSV: {e}")
        return
    
    # Conectar ao banco de dados SQL
    try:
        engine = create_engine("mysql+pymysql://" + \
                               "root" + ":" + \
                               os.getenv('DATABASE_PASS') + "@" + "localhost" + \
                               ":" + "3306" + "/" + "ware_ws_shein" + \
                               "?" + "charset=utf8mb4")
        print("Conexão com o banco de dados estabelecida.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return
    
    # Defina um mapeamento de índice para nome da coluna
    index_to_column_mapping = {
        0: 'warehouse',
        1: 'subpackage_number',
        2: 'order_number',
        3: 'zone_',
        4: 'lane',
        5: 'location',
        6: 'move_and_place_on_shelves',
        7: 'operated_by',
        8: 'operation_time',
        9: 'sector',
        10: 'current_date_',
        11: 'extraction_hour',
        12: 'shift',
    }

    # Renomeie as colunas do DataFrame com base no mapeamento
    df.columns = [index_to_column_mapping.get(i) for i in range(len(df.columns))]
    
    # Insira os dados no banco de dados

    df.to_sql('putaway', engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso na tabela 'putaway'.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        upload_to_sql(file_name)
    else:
        print("Por favor, forneça o nome do arquivo CSV como argumento.")
