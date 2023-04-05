import snowflake.snowpark.functions as F
import datetime


def model(dbt, session):

    df = dbt.ref("menu_item_sales_aggregate")

    # Most recent Friday we have in our sales data
    max_date = df.filter(F.dayofweek(F.col("date"))==5).select(F.max(F.col("date"))).collect()[0][0]

    # get a list of dates from T+9 to T+16
    df_future_dates = df.select(
        F.dateadd("day", F.lit(16), F.col("DATE")).alias("NEW_DATE"),
        F.month(F.dateadd("day", F.lit(16), F.col("DATE"))).alias("month_of_year"),
        F.dayofweek(F.dateadd("day", F.lit(16), F.col("DATE"))).alias("day_of_week"),      
    ).rename(
        F.col("NEW_DATE"), "DATE"
    ).where(
        F.col("DATE") > (max_date + datetime.timedelta(9))
    ).distinct()

    # Get all shifts
    shifts = session.create_dataframe([[0],[1]], schema=["shift"])

    # Get all locations with sales
    locations = df.select("primary_city", "location_id").distinct()

    # Get all items
    items = df.select("menu_type", "menu_item_id", "unit_price").distinct()

    # Cross join to create dimensional table
    df_future_dates = df_future_dates.cross_join(locations).cache_result()
    df_future_dates = df_future_dates.cross_join(items).cache_result()
    df_future_dates = df_future_dates.cross_join(shifts).cache_result()

    # add day of week 
    df_future_dates = df_future_dates.with_column("day_of_week", F.dayofweek(F.col("date")))

    # add month of year
    df_future_dates = df_future_dates.with_column("month_of_year", F.month(F.col("date")))

    # add year
    df_future_dates = df_future_dates.with_column("year", F.year(F.col("date")))

    return df_future_dates