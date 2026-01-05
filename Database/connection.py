import os
from sqlalchemy import create_engine, text

def Database_Connection():
    DB_URL = os.getenv("DATABASE_URL")
    print(DB_URL)
    
    if not DB_URL:
        raise RuntimeError("DATABASE_URL environment variable not set")
    engine = create_engine(DB_URL, pool_pre_ping=True)
    return engine

if __name__== "__main__":
    engine = Database_Connection()
    with engine.connect() as conn:
        print(conn.execute(text("select 1")).scalar())
    Database_Connection()