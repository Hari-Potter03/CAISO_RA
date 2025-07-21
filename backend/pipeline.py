import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import requests
import zipfile
import io
import warnings
warnings.filterwarnings("ignore")

class CAISODataFetcher:
    def __init__(self, base_url="http://oasis.caiso.com/oasisapi/SingleZip"):
        self.base_url = base_url

    def fetch_data(self, query_name, start_date, end_date, tac_area="PGE-TAC"):
        params = {
            "resultformat": "6",  # CSV
            "queryname": query_name,
            "version": "1",
            "startdatetime": start_date,
            "enddatetime": end_date,
            "market_run_id": "RTM",
            "tac_area_name": tac_area
        }
        response = requests.get(self.base_url, params=params)
        if response.ok:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                if not csv_files:
                    print("⚠️ No CSV files found in ZIP.")
                    return pd.DataFrame()
                return pd.read_csv(z.open(csv_files[0]))
        else:
            raise Exception(f"Request failed: {response.status_code}")

class LoadDataProcessor:
    def __init__(self, forecast_df, actual_df):
        self.forecast_df = forecast_df
        self.actual_df = actual_df

    def filter_and_merge(self, tac_area="PGE-TAC"):
        f_df = self.forecast_df[self.forecast_df["TAC_AREA_NAME"] == tac_area]
        a_df = self.actual_df[self.actual_df["TAC_AREA_NAME"] == tac_area]

        f_hourly = f_df.groupby(["OPR_DT", "OPR_HR"])["MW"].mean().reset_index()
        a_hourly = a_df.groupby(["OPR_DT", "OPR_HR"])["MW"].mean().reset_index()

        merged = pd.merge(f_hourly, a_hourly, on=["OPR_DT", "OPR_HR"], suffixes=("_forecast", "_actual"))
        merged["abs_error"] = (merged["MW_forecast"] - merged["MW_actual"]).abs()
        merged["percent_error"] = (merged["abs_error"] / merged["MW_actual"]) * 100
        merged["forecast_bias"] = (merged["MW_forecast"] - merged["MW_actual"])
        merged["hour"] = merged["OPR_HR"]
        merged["day_of_week"] = pd.to_datetime(merged["OPR_DT"]).dt.dayofweek
        return merged