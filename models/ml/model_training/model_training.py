import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
import pandas as pd
import json

def model(dbt, session):

    dbt.config(packages=["pandas"])

    def train_xgboost(
        session: Session,
        training_table: str,
        feature_cols: list,
        target_col: str,
        model_name: str,
        params: dict
    ) -> T.Variant:

        # Import packages
        from xgboost import XGBRegressor
        from joblib import dump

        # Get training data
        df = session.table(training_table).to_pandas()

        # Set inputs X and outputs y
        X = df[feature_cols]
        y = df[target_col]

        # Train model
        #params = eval(eval(params))
        model = XGBRegressor(**params).fit(X, y)

        # Get feature importance
        feature_gain = model.get_booster().get_score(importance_type="gain")
        

        # Save model
        dump(model, "/tmp/" + model_name)
        session.file.put(
            "/tmp/" + model_name,
            "@FORECAST_STAGE",
            auto_compress=False,
            overwrite=True
        )

        # Return feature importance
        return feature_gain

    # Specify inputs
    tuning_df = dbt.ref("hyperparameter_tuning_df")
    tune_results_df = dbt.ref("hyperparameter_tuning_results")
    training_table = dbt.ref("demand_forecast_train_demo")
    train_df = dbt.ref("demand_forecast_train")

    best_param_id = tune_results_df.sort(F.col('RMSE').asc()).select('hp_id').collect()[0][0]
    feature_cols = train_df.drop("quantity").columns

    params = tuning_df.drop("feature_vector").filter(F.col("HP_ID")==best_param_id).distinct()
    params = params.drop('HP_ID').to_pandas().iloc[0,:].to_dict()
    target_col = "QUANTITY"
    model_name = "xgboost_demand_forecast_model.sav"

    train_xgboost_snowflake = session.sproc.register(
        func=train_xgboost,
        name="sproc_train_xgboost",
        is_permanent=True,
        replace=True,
        stage_location="@FORECAST_STAGE",
        packages=["snowflake-snowpark-python", "xgboost", "joblib"]
    )

    # Call the training stored procedure
    feature_importance = train_xgboost_snowflake(
        session,
        training_table.table_name,
        feature_cols,
        target_col,
        model_name,
        params
    )   

    return  pd.DataFrame([json.loads(feature_importance)])


