import datetime
import snowflake.snowpark.functions as F


def model(dbt, session):

    dbt.config(snowflake_warehouse="TASTY_SNOWPARK_WH")

    orders_df = dbt.ref("fct_orders")

    # Define features
    order_features = [
        "date",
        "shift",
        "primary_city",
        "location_id",
        "truck_id",
        "menu_type",
        "menu_item_id",
        "unit_price",
    ]

    # Define target
    target = ["quantity"]
    max_date = datetime.date(2022, 10, 30)

    # Create DataFrame and define AM/PM shifts
    df_base = (
        orders_df
        .where(F.col("date") <= max_date)
        .with_column(
            "shift",
            F.iff(F.builtin("DATE_PART")("HOUR", F.col("order_ts")) < "15", "AM", "PM"),
        )
    )

    # Filter out 'extra' items that have no cost
    df_base = df_base.filter(df_base.menu_item_id < 1000)

    # Aggregate
    df_base = df_base.group_by(order_features).agg(F.sum(*target).alias("quantity"))

    # Calculate sales
    df_base = df_base.with_column(
        "sales", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    return df_base