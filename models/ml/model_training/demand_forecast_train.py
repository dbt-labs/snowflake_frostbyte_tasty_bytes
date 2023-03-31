import snowflake.snowpark.functions as F

def model(dbt, session):

    df = dbt.ref("feature_engineer_prepare_data")

    df = df.where(F.col("split_type") == "train").drop("split_type")

    return df
