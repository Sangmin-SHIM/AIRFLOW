import os 
from sqlalchemy import create_engine

def get_db_connection():
    db_user = os.getenv('SOCIAL_USER', 'postgres')
    db_pass = os.getenv('SOCIAL_PASS', 'postgres')
    db_host = 'social_postgres'
    db_port = '5432'
    db_name = 'social_data'
    
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
    return engine