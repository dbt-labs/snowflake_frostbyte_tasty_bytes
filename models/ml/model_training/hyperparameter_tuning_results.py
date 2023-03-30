import snowflake.snowpark.functions as F
import pandas as pd


def model(dbt, session):

    dbt.config(packages=["pandas"])

    tuning_df = dbt.ref("hyperparameter_tuning_df")

    tune_results = tuning_df.sample(n=50000).select(
        F.col("hp_id"),
        (
            F.table_function("hyperparameter_tuning")(
                tuning_df["max_depth"],
                tuning_df["learning_rate"],
                tuning_df["gamma"],
                tuning_df["n_estimators"],
                tuning_df["feature_vector"]
            ).over(partition_by="hp_id")
        )
    ).sort(F.col('RMSE').asc())
    best_param_id = tune_results.select('hp_id').collect()[0][0]

    params = tuning_df.drop("feature_vector").filter(F.col("HP_ID")==best_param_id).distinct()
    params = params.drop('HP_ID').to_pandas()

    return params