import snowflake.snowpark.functions as F
import datetime

def get_last_year_same_month_sales(df_predictions, df_historical, group_list):
    
    group_label = group_list[1] if len(group_list)==2 else group_list[1] + "_" + group_list[-1]
    column_name = "prev_year_month_sales_" + group_label.replace("_id", "").replace("primary_","")
    
    group_list.append("month_of_year")
    group_list.append("year")
    
    # get summary
    grouped_df = df_historical.group_by(
        group_list
    ).agg(
        F.avg(df_historical.sales).alias(column_name)
    )

    # decrement year
    df_predictions = df_predictions.with_column("year", F.col("year") - 1)
    
    # join df and summary
    df_predictions = df_predictions.join(
        right=grouped_df,
        on=group_list,
        how="left"
    )
    
    # correct year column
    df_predictions = df_predictions.with_column("year", F.col("year") + 1)
    
    del group_list[-2:]
    
    return df_predictions


def get_day_of_week_trends(df_predictions, df_historical, group_list,max_date):
    
    group_label = group_list[1] if len(group_list)==2 else group_list[1] + "_" + group_list[-1]
    column_name = "day_of_week_avg_" + group_label.replace("_id", "").replace("primary_","")
    
    group_list.append("day_of_week")
    
    df_historical = df_historical.filter(F.col("date") < max_date - datetime.timedelta(days=4))
    # get summary
    grouped_df = df_historical.group_by(
        group_list
    ).agg(
        F.avg(df_historical.sales).alias(column_name)
    )
    
    # join df and summary
    df_predictions = df_predictions.join(
        right=grouped_df,
        on=group_list,
        how="left"
    )
    
    del group_list[-1:]
    
    return df_predictions

def model(dbt, session):

    df_future_dates = dbt.ref("explode_sales_features")
    df = dbt.ref("menu_item_sales_aggregate")

    # Most recent day we have sales data (should be today / day we predict)
    max_date = df.select(F.max(F.col("date"))).collect()[0][0]

    grouping_lists = [
        ["shift", "primary_city"],
        ["shift", "location_id"],
        ["shift", "primary_city", "menu_item_id"],
        ["shift", "primary_city", "menu_type"],
    ]

    for grouping in grouping_lists:
        df_future_dates = get_last_year_same_month_sales(df_future_dates, df, grouping).cache_result()


    for grouping in grouping_lists:
        df_future_dates = get_day_of_week_trends(df_future_dates, df, grouping, max_date).cache_result()


    return df_future_dates