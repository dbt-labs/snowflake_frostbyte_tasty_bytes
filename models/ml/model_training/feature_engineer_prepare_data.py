import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import lit
import datetime

def model(dbt, session):

    df = dbt.ref("demand_forecast_training_base")
    orders_df = dbt.ref("fct_orders")

    # Get earliest date
    min_date = df.select(F.dateadd("year", F.lit(1), F.min("date"))).collect()[0][0]

    # Filter out the first year in the data
    df = df.filter(F.col("date") >= min_date)

    # Join in third party data
    df_locations = orders_df.select("location_id", "latitude", "longitude").distinct()
    df = df.join(df_locations, "location_id")

    df_holidays = orders_df.select("date", "primary_city", "holiday_flag").distinct()
    df = df.join(df_holidays, ["date", "primary_city"])


    # Encode
    df = df.with_column("shift", F.iff(F.col("shift") == "AM", 1, 0))

    # Split into training and testing
    max_date = df.select(F.max("date")).collect()[0][0]
    df = df.with_column("split_type",
                        F.iff(F.col("date") <= (max_date - datetime.timedelta(days=30)),
                                F.lit("train"),
                                F.lit("test")))
    
    # Drop ID columns
    df = df.drop("date",
            "primary_city",
            "location_id",
            "menu_type",
            "menu_item_id",)

    return df