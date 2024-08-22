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
    
    local_file_path = os.path.join('/opt/airflow/data_lake/Gold/Picking', file_name)
    delimiter = detect_delimiter(local_file_path)
    
    # Carregar os dados do arquivo CSV
    try:
        df = pd.read_csv(local_file_path, delimiter=delimiter)
        print(f"Arquivo CSV '{file_name}' carregado com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar o arquivo CSV: {e}")
        return
    
    # Conectar ao banco de dados SQL
    try:
        engine = create_engine(
            f"mysql+pymysql://root:{os.getenv('DATABASE_PASS')}@localhost:3306/ware_ws_shein?charset=utf8mb4"
        )
        print("Conexão com o banco de dados estabelecida.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return
    
    # Verifique se as colunas do DataFrame correspondem à tabela no banco de dados
    print(f"Colunas no DataFrame: {df.columns}")
    
    # Inserir os dados no banco de dados

    df.to_sql('uph_per_hour', engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso na tabela 'uph_per_hour'.")
    
    # Apagar o arquivo original na Bronze (se necessário)
    os.remove(local_file_path)
    print(f"Original file {local_file_path} has been deleted.")
    


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        upload_to_sql(file_name)
    else:
        print("Por favor, forneça o nome do arquivo CSV como argumento.")
