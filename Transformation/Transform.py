import pandas as pd
import re
from DataIngestion.extract import Fetch_Data


def parse_month_columns(col_name: str) -> str:
    """
    Convert MO012016 → 2016-01
    """
    match = re.match(r"MO(\d{2})(\d{4})", col_name)
    if match:
        month, year = match.groups()
        return f"{year}-{month}"
    return col_name

def json_to_dataframe(json_Data: dict) -> pd.DataFrame:
    data = json_Data["SASTableData+P0142_7"]
    df = pd.DataFrame(data)
    return df

def clean_dataFrame(raw_df: pd.DataFrame) -> pd.DataFrame:
    return raw_df.rename(columns=parse_month_columns)




if __name__ == "__main__":
    json_data = Fetch_Data()
    df = json_to_dataframe(json_data)
    cleaned_df = clean_dataFrame(df)
    print(cleaned_df.head())