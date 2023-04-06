import snowflake.snowpark.functions as F

def model(dbt, session):

    df_base = dbt.ref("features_sales_aggregate")   
   
    # Add day of week 
    df = df_base.with_column("day_of_week", F.dayofweek(F.col("date")))

    # Add month of year
    df = df.with_column("month_of_year", F.month(F.col("date")))

    # Add year
    df = df.with_column("year", F.year(F.col("date")))

    return df