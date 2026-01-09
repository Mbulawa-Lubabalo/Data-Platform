from sqlalchemy import text
from Database.connection import Database_Connection
from DataIngestion.extract import Fetch_Data
from Transformation.Transform import transform_json_to_df
import pandas as pd


def Create_Tables(engine):
    """
    Create the core data warehouse tables in the public schema.

    This function creates the following tables if they do not already exist:
    - dim_series: Stores high-level time series metadata.
    - dim_indicator: Stores indicator-level metadata linked to a series.
    - dim_date: Date dimension table used for time-based analysis.
    - fact_index: Fact table storing index values by indicator and date.

    The function executes all CREATE TABLE statements inside a single
    database transaction to ensure atomicity.

    
    :param engine: sqlalchemy.engine.Engine
        A SQLAlchemy Engine instance connected to the target PostgreSQL
        (Supabase) database.

    returns: None
    """

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


def insert_dim_date(engine, df):
    date_df = (
        df[[
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "year_month"
        ]]
        .drop_duplicates()
        .copy()
    )

    date_df["date"] = pd.to_datetime(date_df["date"])
    date_df["date_key"] = date_df["date"].dt.strftime("%Y%m%d").astype(int)

    sql = text("""
        INSERT INTO public.dim_date (
            date_key,
            date,
            year,
            quarter,
            month,
            month_name,
            year_month
        )
        VALUES (
            :date_key,
            :date,
            :year,
            :quarter,
            :month,
            :month_name,
            :year_month
        )
        ON CONFLICT (date_key) DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(sql, date_df.to_dict("records"))




def insert_fact_index(engine, df):
    fact_df = df.copy()
    fact_df["date"] = pd.to_datetime(fact_df["date"])
    fact_df["date_key"] = fact_df["date"].dt.strftime("%Y%m%d").astype(int)

    sql = text("""
        INSERT INTO public.fact_index (
            indicator_key,
            date_key,
            index_value
        )
        SELECT
            i.indicator_key,
            :date_key,
            :index_value
        FROM public.dim_indicator i
        WHERE i.indicator_code = :indicator_code
        ON CONFLICT (indicator_key, date_key) DO NOTHING
    """)

    records = (
        fact_df[[
            "indicator_code",
            "date_key",
            "index_value"
        ]]
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(sql, records)




if __name__== "__main__":
    engine = Database_Connection()
    json_data = Fetch_Data()
    df = transform_json_to_df(json_data["SASTableData+P0142_7"])

    Create_Tables(engine)
    print("Tables created")
    insert_dim_series(engine, df)
    print("Inserted series")
    insert_dim_indicator(engine, df)
    print("Inserted indicator")
    insert_dim_date(engine, df)
    print("Inserted date")
    insert_fact_index(engine, df)
    print("Inserted index")
