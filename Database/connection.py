import os
from sqlalchemy import create_engine

def Database_Connection():
    DB_URL = os.getenv("DATABASE_URL")
    
    if not DB_URL:
        raise RuntimeError("DATABASE_URL environment variable not set")
    engine = create_engine(DB_URL, pool_pre_ping=True)
    return engine

if __name__== "__main__":
    Database_Connection()