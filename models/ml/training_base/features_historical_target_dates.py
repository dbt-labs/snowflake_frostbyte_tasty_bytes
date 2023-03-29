import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

# Get average monthly sales from previous year
def get_last_year_same_month_sales(df, group_list):
    
    group_label = group_list[1] if len(group_list)==2 else group_list[1] + "_" + group_list[-1]
    column_name = "prev_year_month_sales_" + group_label.replace("_id", "").replace("primary_","")
    
    
    group_list.append("month_of_year")
    group_list.append("year")
    
    # get summary
    grouped_df = df.group_by(
        group_list
    ).agg(
        F.sum(df.sales).alias(column_name)
    )
    
    # rename function columns
    #grouped_df = grouped_df.rename("AVG(SALES)", column_name)
    
    # decrement year
    df = df.with_column("year", F.col("year") - 1)
    
    # join df and summary
    df = df.join(
        right=grouped_df,
        on=group_list,
        how="left"
    )
    
    # correct year column
    df = df.with_column("year", F.col("year") + 1)
    
    del group_list[-2:]
    
    return df

# Get day of week trends up to 3 weeks before target date
def get_day_of_week_trends(df, group_list):
    
    group_label = group_list[1] if len(group_list)==2 else group_list[1] + "_" + group_list[-1]
    column_name = "day_of_week_avg_" + group_label.replace("_id", "").replace("primary_","")
    
    group_list.append("day_of_week")
    
    window_with_grouping = (
        Window.partition_by(group_list)
        .order_by("date")
        .rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW - 3)
    )

    df = df.with_column(
        column_name, 
        F.avg("sales").over(window_with_grouping)
    )
    
    return df


def model(dbt, session):

    df_future = dbt.ref("features_future_dates")

    # Define aggregation grouping
    grouping_lists = [
        ["shift", "primary_city"],
        ["shift", "location_id"],
        ["shift", "primary_city", "menu_item_id"],
        ["shift", "primary_city", "menu_type"],
    ]

    # Create features

    for grouping in grouping_lists:
        df_future = get_last_year_same_month_sales(df_future, grouping)
        
    for grouping in grouping_lists:
        df_future = get_day_of_week_trends(df_future, grouping)

    return df_future