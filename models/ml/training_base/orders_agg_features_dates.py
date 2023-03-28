import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )

    df_base = dbt.ref("orders_agg_features")   
   
   
    # Add day of week 
    df = df_base.with_column("day_of_week", F.dayofweek(F.col("date")))

    # Add month of year
    df = df.with_column("month_of_year", F.month(F.col("date")))

    # Add year
    df = df.with_column("year", F.year(F.col("date"))).cache_result()

    return df