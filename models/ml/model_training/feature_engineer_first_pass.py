import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )


    df_future = dbt.ref("features_demand_forecast_training_base")

    # Get earliest date
    min_date = df.select(F.dateadd("year", F.lit(1), F.min("date"))).collect()[0][0]

    # Filter out the first year in the data
    df = df.filter(F.col("date") >= min_date)

    # Drop ID columns
    df = df.drop("date",
            "primary_city",
            "location_id",
            "truck_id",
            "menu_type",
            "menu_item_id",)

    # Encode
    df = df.with_column("shift", F.iff(F.col("shift") == "AM", 1, 0))
    train_df, test_df = df.randomSplit([0.8, 0.2])

    return df