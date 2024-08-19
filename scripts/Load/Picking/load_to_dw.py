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
    
    local_file_path = os.path.join('/opt/airflow/data_lake/Silver/Picking', file_name)
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
        1: 'picking_group_number',
        2: 'picking_task_number',
        3: 'picking_methods',
        4: 'type_',
        5: 'picking_groups_automated',
        6: 'consolid_ord_num',
        7: 'combined_packing_number',
        8: 'subpackage_number',
        9: 'wheter_short_picking',
        10: 'picking_location',
        11: 'lane',
        12: 'picking_area',
        13: 'picking_container',
        14: 'status',
        15: 'create_by',
        16: 'task_criation_time',
        17: 'task_pick_up_time',
        18: 'picker',
        19: 'picking_time',
        20: 'voided_by',
        21: 'voided_time',
        22: 'flag_cancel',
        23: 'sector',
        24: 'current_date_',
        25: 'extraction_hour',
        26: 'shift'
    }

    # Renomeie as colunas do DataFrame com base no mapeamento
    df.columns = [index_to_column_mapping.get(i) for i in range(len(df.columns))]
    
    # Insira os dados no banco de dados

    df.to_sql('picking', engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso na tabela 'picking'.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        upload_to_sql(file_name)
    else:
        print("Por favor, forneça o nome do arquivo CSV como argumento.")
