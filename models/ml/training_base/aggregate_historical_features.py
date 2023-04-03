def model(dbt, session):

    df_base = dbt.ref("features_sales_aggregate")

    ## Create Dimensional Table
    df = df_base

    # Get all dates
    dates = df.select('date').distinct().cache_result()

    # Get all shifts
    shifts = session.create_dataframe([["AM"],["PM"]], schema=["shift"]).cache_result()

    # Get all locations with sales
    locations = df.select("primary_city", "location_id").distinct().cache_result()

    # Get all items
    items = df.select("menu_type", "menu_item_id").distinct().cache_result()

    # Cross join to create dimensional table
    df_dim = locations.cross_join(dates).cache_result()
    df_dim = df_dim.cross_join(items).cache_result()
    df_dim = df_dim.cross_join(shifts).cache_result()

    # Join dimensional table with aggregate data
    df_expanded = df_dim.join(df, ["date", "shift", "location_id", "menu_item_id"], "left").select(
        [df_dim.col(c).alias(c) for c in df_dim.columns] + ["unit_price", "quantity", "sales"]
    )

    return df_expanded