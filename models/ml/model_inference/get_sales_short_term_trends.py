import snowflake.snowpark.functions as F
import datetime


def get_short_term_trends(df_predictions, df_historical, partitions, agg_period, max_date):
    max_date = df_historical.select(F.max(F.col("date"))).collect()[0][0]
    
    # filter number of days 7, 14, 28 in the past
    for days in agg_period:
        
        df_filtered = df_historical.filter(df_historical.date > (max_date - datetime.timedelta(days)))
        
        for p in partitions:
            for i in range(1,len(p)):
                partition = p[0:i+1]

                # Define aggregate - Quantity at menu item level, sales for higher levels
                agg = "quantity" if partition[-1] == "menu_item_id" else "sales"
                partition_label = partition[1] if len(partition)==2 else partition[1] + "_" + partition[-1]
                partition_label = partition_label.replace("_id", "").replace("primary_","")
                column_name = "avg_" + agg + "_" + str(days) + "d_" + partition_label

                # get summary metrics by grouping on partitions
                grouped_df = df_filtered.group_by(
                    partition
                ).agg(
                    F.avg(agg).alias(column_name)
                )

                # future dates left join aggregate (date not relevant)
                df_predictions = df_predictions.join(
                    right=grouped_df,
                    on=partition,
                    how="left"
                ).cache_result()
    
    return df_predictions


def get_all_time_trends(df_predictions, df_historical, partitions):
    for p in partitions:
        for i in range(1,len(p)):
            partition = p[0:i+1]
            
            # Define aggregate - Quantity at menu item level, sales for higher levels
            agg = "quantity" if partition[-1] == "menu_item_id" else "sales"
            partition_label = partition[1] if len(partition)==2 else partition[1] + "_" + partition[-1]
            partition_label = partition_label.replace("_id", "").replace("primary_","")
            column_name = "avg_" + agg + "_" + partition_label

            # get summary metrics by grouping on partitions
            grouped_df = df_historical.group_by(
                partition
            ).agg(
                F.avg(agg).alias(column_name)
            )

            # future dates left join aggregate (date not relevant)
            df_predictions = df_predictions.join(
                right=grouped_df,
                on=partition,
                how="left"
            ).cache_result()

    return df_predictions


def model(dbt, session):

    df_future_dates = dbt.ref("get_sales_historical_trends")
    df = dbt.ref("menu_item_sales_aggregate")
    df_locations = dbt.ref("fct_orders").select("location_id", "latitude", "longitude").distinct()
    df_holidays = dbt.ref("dim_holidays").select("date", "city", "holiday_flag").distinct()

    # Most recent day we have sales data (should be today / day we predict)
    max_date = df.select(F.max(F.col("date"))).collect()[0][0]

    agg_period = [
        7,
        14,
        28,
    ]

    partitions = [
        ["shift", "location_id", "menu_type", "menu_item_id"],
        ["shift", "primary_city", "menu_type", "menu_item_id"],
        ["shift", "menu_type"],
        ["shift", "menu_item_id"],
    ]


    df_future_dates = get_short_term_trends(df_future_dates, df, partitions, agg_period, max_date)

    df_future_dates = get_all_time_trends(df_future_dates, df, partitions).cache_result()

    # Join in third party data
    df_future_dates = df_future_dates.join(df_locations, "location_id")

    df_future_dates = (df_future_dates.rename(F.col("primary_city"), "CITY")
                                                .join(df_holidays, ["date","city"], "left")
                        )
    df_future_dates = df_future_dates.na.fill({"holiday_flag":0})

    return df_future_dates