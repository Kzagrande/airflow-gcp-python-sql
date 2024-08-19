# db_connection.py

import os
from sqlalchemy import create_engine

def get_engine():
    try:
        engine = create_engine(
            f"mysql+pymysql://root:{os.getenv('DATABASE_PASS')}@localhost:3306/ware_ws_shein?charset=utf8mb4"
        )
        print("Conexão com o banco de dados estabelecida.")
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
