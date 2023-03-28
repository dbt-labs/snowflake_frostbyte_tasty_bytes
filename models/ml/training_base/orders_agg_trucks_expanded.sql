import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )
        
    df_dim = dbt.ref("orders_shifts_loc_items_dim")

    # Get trucks
    trucks = df.select("truck_id", "primary_city", "menu_type").distinct().cache_result()

    # Join in truck id
    df_dim = df_dim.join(
        trucks,
        (df_dim.primary_city == trucks.primary_city)
        & (df_dim.menu_type == trucks.menu_type),
    ).select(
        "date",
        "shift",
        df_dim.primary_city.alias("primary_city"),
        "location_id",
        "latitude",
        "longitude",
        "truck_id",
        df_dim.menu_type.alias("menu_type"),
        "menu_item_id",
    ).cache_result()


    df_expanded = df_dim.join(df, ["date", "shift", "location_id", "menu_item_id"]).select(
        [df_dim.col(c).alias(c) for c in df_dim.columns] + ["unit_price", "quantity", "sales"]
    ).cache_result()

    return df_expanded