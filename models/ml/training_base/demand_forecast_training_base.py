import snowflake.snowpark.functions as F

def model(dbt, session):

    df_future = dbt.ref("features_historical_target_dates")

    # Target date feature table
    df_future = df_future.with_column("forecast_date", F.dateadd("day", -(F.col("day_of_week") + 9), F.col("date")))

    # Forecast date feature table
    df_hist = dbt.ref("forecast_training_historical")

    # Define feautres

    future_features = [
        "date",
        "shift",
        "primary_city",
        "location_id",
        #"truck_id",
        "menu_type",
        "menu_item_id",
        "month_of_year",
        "day_of_week",
        "unit_price",
        "prev_year_month_sales_city",
        "prev_year_month_sales_location",
        "prev_year_month_sales_city_menu_item",
        "prev_year_month_sales_city_menu_type",
        "day_of_week_avg_city",
        "day_of_week_avg_location",
        "day_of_week_avg_city_menu_item",
        "day_of_week_avg_city_menu_type",
    ]

    target = ["quantity"]

    hist_features = [
        "avg_sales_7d_location",
        "avg_sales_14d_location",
        "avg_sales_28d_location",
        "avg_sales_location",
        "avg_sales_7d_location_menu_type",
        "avg_sales_14d_location_menu_type",
        "avg_sales_28d_location_menu_type",
        "avg_sales_location_menu_type",
        "avg_quantity_7d_location_menu_item",
        "avg_quantity_14d_location_menu_item",
        "avg_quantity_28d_location_menu_item",
        "avg_quantity_location_menu_item",
        "avg_sales_7d_city",
        "avg_sales_14d_city",
        "avg_sales_28d_city",
        "avg_sales_city",
        "avg_sales_7d_city_menu_type",
        "avg_sales_14d_city_menu_type",
        "avg_sales_28d_city_menu_type",
        "avg_sales_city_menu_type",
        "avg_quantity_7d_city_menu_item",
        "avg_quantity_14d_city_menu_item",
        "avg_quantity_28d_city_menu_item",
        "avg_quantity_city_menu_item",
        "avg_sales_7d_menu_type",
        "avg_sales_14d_menu_type",
        "avg_sales_28d_menu_type",
        "avg_sales_menu_type",
        "avg_quantity_7d_menu_item",
        "avg_quantity_14d_menu_item",
        "avg_quantity_28d_menu_item",
        "avg_quantity_menu_item",
    ]

    df_training = df_future.join(
        df_hist, 
        (
            (df_future.forecast_date == df_hist.date)
            & (df_future.shift == df_hist.shift)
            & (df_future.location_id == df_hist.location_id)
            & (df_future.menu_item_id == df_hist.menu_item_id)
        ),
        "left"
    ).select(
        [df_future.col(c).alias(c) for c in future_features + target]
        + [df_hist.col(c).alias(c) for c in hist_features]
    ).cache_result()

    # Get earliest date to include in the data
    min_date = df_training.select(F.dateadd("year", F.lit(1), F.min("date"))).collect()[0][0]

    # Filter out the first year in the data
    df_training = df_training.filter(F.col("date") >= min_date)

    return df_training