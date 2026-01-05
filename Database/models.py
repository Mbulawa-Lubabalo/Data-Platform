from sqlalchemy import text
from Database.connection import Database_Connection


def Create_Tables(engine):
    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS public.dim_series (
                    series_key BIGSERIAL PRIMARY KEY,
                    series_code TEXT UNIQUE NOT NULL, 
                    series_name TEXT NOT NULL,          
                    frequency        TEXT NOT NULL,       
                    base_period      TEXT
                );
                 
                CREATE TABLE IF NOT EXISTS public.dim_indicator (
                    indicator_key BIGSERIAL PRIMARY KEY,
                    series_key BIGINT NOT NULL
                        REFERENCES public.dim_series(series_key),
                 
                    indicator_code TEXT UNIQUE NOT NULL,  
                    category       TEXT NOT NULL,       
                    subcategory    TEXT,                 
                    unit           TEXT NOT NULL     
                 );
                 
                CREATE TABLE IF NOT EXISTS public.dim_date (
                    date_key    INTEGER PRIMARY KEY, 
                    date        DATE UNIQUE NOT NULL,
                    year        SMALLINT NOT NULL,
                    quarter     SMALLINT NOT NULL,
                    month       SMALLINT NOT NULL,
                    month_name  TEXT NOT NULL,
                    year_month  TEXT NOT NULL        
                );
                 
                CREATE TABLE IF NOT EXISTS public.fact_index (
                    indicator_key BIGINT NOT NULL
                        REFERENCES public.dim_indicator(indicator_key),

                    date_key INTEGER NOT NULL
                        REFERENCES public.dim_date(date_key),

                    index_value NUMERIC(10,2) NOT NULL,

                    load_timestamp TIMESTAMP DEFAULT now(),

                    PRIMARY KEY (indicator_key, date_key)
                );

            """)
        )


if __name__== "__main__":
    engine = Database_Connection()
    Create_Tables(engine)