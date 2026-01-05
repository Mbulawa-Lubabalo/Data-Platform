from sqlalchemy import text
from Database.connection import Database_Connection


def Create_Tables(engine):
    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS public.dim_publication (
                    publication_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    frequency TEXT,
                    base_period TEXT
                );
            """)
        )


if __name__== "__main__":
    engine = Database_Connection()
    Create_Tables(engine)