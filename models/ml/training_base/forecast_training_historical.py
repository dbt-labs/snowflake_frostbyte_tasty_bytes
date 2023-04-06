from snowflake.snowpark import Window
import snowflake.snowpark.functions as F

def model(dbt, session):

    df_expanded = dbt.ref("aggregate_historical_features")

    # Get rolling features
    agg_period = [7, 14, 28]

    partitions = [["date", "shift", "location_id", "menu_type", "menu_item_id"],
                ["date", "shift", "primary_city", "menu_type", "menu_item_id"],
                ["date", "shift", "menu_type"],
                ["date", "shift", "menu_item_id"]]

    df_hist = df_expanded
    original_columns = df_expanded.columns
    for p in partitions:
        for i in range(2,len(p)):
            partition = p[0:i+1]
            # Define aggregate - Quantity at menu item level, sales for higher levels
            agg = "quantity" if partition[-1] == "menu_item_id" else "sales"
            # Aggregate table
            df_agg = df_expanded.group_by(partition).agg(F.sum(agg).alias(agg))

            # Create partition label for new columns
            partition_label = partition[2] if len(partition)==3 else partition[2] + "_" + partition[-1]
            partition_label = partition_label.replace("_id", "").replace("primary_","")

            new_columns = []

            for days in agg_period:
                window = (
                    Window.partition_by(partition[1:])
                    .order_by("date")
                    .rows_between(Window.CURRENT_ROW - days, Window.CURRENT_ROW)
                )

                column_name = "avg_" + agg + "_" + str(days) + "d_" + partition_label
                df_agg = df_agg.with_column(column_name, F.avg(agg).over(window))
                new_columns.append(column_name)

            # Create partition label for new columns
            partition_label = partition[2] if len(partition)==3 else partition[2] + "_" + partition[-1]
            partition_label = partition_label.replace("_id", "").replace("primary_","")

            window = (
                Window.partition_by(partition[1:])
                .order_by("date")
                .rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
            )

            column_name = "avg_" + agg + "_" + partition_label
            df_agg = df_agg.with_column(column_name, F.avg(agg).over(window))
            new_columns.append(column_name)

            df_hist = df_hist.join(df_agg, partition).select([df_hist.col(c).alias(c) for c in original_columns] + new_columns)
            df_hist = df_hist.cache_result()
            original_columns = df_hist.columns

    return df_hist