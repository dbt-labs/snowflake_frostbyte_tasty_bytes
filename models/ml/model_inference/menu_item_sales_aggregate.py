import snowflake.snowpark.functions as F


def model(dbt, session):

    orders_df = dbt.ref("fct_orders")

    # Define features
    order_features = [
        "date",
        "shift",
        "primary_city",
        "location_id",
        "menu_type",
        "menu_item_id",
        "unit_price",
    ]

    # Define target
    target = ["quantity"]

    # Create DataFrame and define AM/PM shifts
    df = orders_df.with_column(
        "shift", F.iff(F.builtin("DATE_PART")("HOUR", F.col("order_ts")) < "15", 1, 0)
    )

    # Aggregate
    df = df.group_by(order_features).agg(F.sum(*target).alias("quantity"))

    # Calculate sales
    df = df.with_column("sales", F.round(F.col("quantity") * F.col("unit_price"),2))

    # add day of week 
    df = df.with_column("day_of_week", F.dayofweek(F.col("date")))

    # add month of year
    df = df.with_column("month_of_year", F.month(F.col("date")))

    # add year
    df = df.with_column("year", F.year(F.col("date")))

    return df