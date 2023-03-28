import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )

    df_base = dbt.ref("orders_agg_features")

    ## Create Dimensional Table
    df = df_base

    # Get all dates
    dates = df.select('date').distinct().cache_result()

    # Get all shifts
    shifts = session.create_dataframe([["AM"],["PM"]], schema=["shift"]).cache_result()

    # Get all locations with sales
    locations = df.select("primary_city", "location_id", "latitude", "longitude").distinct().cache_result()

    # Get all items
    items = df.select("menu_type", "menu_item_id").distinct().cache_result()

    # Cross join to create dimensional table
    df_dim = locations.cross_join(dates).cache_result()
    df_dim = df_dim.cross_join(items).cache_result()
    df_dim = df_dim.cross_join(shifts).cache_result()

    return df_dim