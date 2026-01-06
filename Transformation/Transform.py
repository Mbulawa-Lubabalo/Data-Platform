import pandas as pd
from DataIngestion.extract import Fetch_Data
from datetime import date


def transform_json_to_df(json_data: list[dict]) -> pd.DataFrame:
    records = []

    COLUMN_MAP = {
        "H01": "series_code",
        "H02": "series_name",
        "H03": "indicator_code",
        "H04": "category",
        "H05": "subcategory",
        "H25": "frequency",
        "H17": "unit",
        "H18": "base_period"
        }

    for row in json_data:
        base_fields = {
            target_col: row.get(source_col)
            for source_col, target_col in COLUMN_MAP.items()
        }

        for key, value in row.items():
            if key.startswith("MO"):
                month = int(key[2:4])
                year = int(key[4:8])

                d = date(year, month, 1)

                records.append({
                    **base_fields,
                    "date": d,
                    "year": year,
                    "quarter": (month - 1) // 3 + 1,
                    "month": month,
                    "month_name": d.strftime("%B"),
                    "year_month": f"{year}-{month:02d}",
                    "index_value": value
                })

    return pd.DataFrame(records)


if __name__ == "__main__":
    json_data = Fetch_Data()
    df = transform_json_to_df(json_data["SASTableData+P0142_7"])

    print(df.head())
