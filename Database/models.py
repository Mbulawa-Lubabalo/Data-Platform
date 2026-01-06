from sqlalchemy import text
from Database.connection import Database_Connection
from DataIngestion.extract import Fetch_Data
from Transformation.Transform import json_to_dataframe, clean_dataFrame, Transformed_df


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


def insert_dim_series(engine, df):
    sql = text("""
        INSERT INTO public.dim_series (series_code, series_name, frequency, base_period)
        SELECT DISTINCT
            :series_code,
            :series_name,
            :frequency,
            :base_period
        ON CONFLICT (series_code) DO NOTHING
    """)

    records = (
        df[["series_code", "series_name", "frequency", "base_period"]]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(sql, records)



def insert_dim_indicator(engine, df):
    sql = text("""
        INSERT INTO public.dim_indicator (
            series_key,
            indicator_code,
            category,
            subcategory,
            unit
        )
        SELECT
            s.series_key,
            :indicator_code,
            :category,
            :subcategory,
            :unit
        FROM public.dim_series s
        WHERE s.series_code = :series_code
        ON CONFLICT (indicator_code) DO NOTHING
    """)

    records = (
        df[[
            "series_code",
            "indicator_code",
            "category",
            "subcategory",
            "unit"
        ]]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(sql, records)




if __name__== "__main__":
    engine = Database_Connection()
    df = Transformed_df()

    Create_Tables(engine)
    insert_dim_series(engine, df)
    insert_dim_indicator(engine, df)