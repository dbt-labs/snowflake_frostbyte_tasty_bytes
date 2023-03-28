import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )

    orders_ref = dbt.ref("orders_v")

    ## Create Aggregate Table
    # Define features
    order_features = [
        "date",
        "shift",
        "primary_city",
        "location_id",
        "latitude",
        "longitude",
        "truck_id",
        "menu_type",
        "menu_item_id",
        "unit_price",
    ]

    # Define target
    target = ["quantity"]

    # Create DataFrame and define AM/PM shifts
    df_base = (
        session.table(orders_ref)
        .where(F.col("date") <= max_date)
        .with_column(
            "shift",
            F.iff(F.builtin("DATE_PART")("HOUR", F.col("order_ts")) < "15", "AM", "PM"),
        )
    )

    # Aggregate
    df_base = df_base.group_by(order_features).agg(F.sum(*target).alias("quantity"))

    # Calculate sales
    df_base = df_base.with_column(
        "sales", F.round(F.col("quantity") * F.col("unit_price"), 2)
    ).cache_result()

    return df_base