import snowflake.snowpark.functions as F


def model(dbt, session):

    df_future_dates = dbt.ref("get_sales_short_term_trends")

    #this is to reference the UDF creation 
    demand_forecast_udf = dbt.ref("demand_forecast_train_metrics")

    feature_cols = df_future_dates.drop(
        "MENU_ITEM_ID",
        "MENU_TYPE",
        "CITY",
        "LOCATION_ID",
        "DATE",
        "YEAR",
    ).columns


    df_predictions = df_future_dates.select(
        F.dateadd("day", -(F.col("day_of_week") + 9), F.col("date")).alias("forecast_execution_date"),
        "location_id",
        "date",
        "shift",
        "menu_item_id",
        F.call_udf("udf_xgboost_predict_demand", [F.col(c) for c in feature_cols]).alias("prediction")
    )

    return df_predictions