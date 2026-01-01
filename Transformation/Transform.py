import pandas as pd
from DataIngestion.extract import Fetch_Data

def run_transform():
    data = Fetch_Data()
    data = data["SASTableData+P0142_7"]
    df = pd.DataFrame(data)
    print(df.head())


if __name__ == "__main__":
    run_transform()