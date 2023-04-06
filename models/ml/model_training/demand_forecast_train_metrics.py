import pandas as pd
import cachetools
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

def model(dbt, session):

    dbt.config(packages=["pandas","cachetools"])

    train_df = dbt.ref("demand_forecast_train")
    test_df = dbt.ref("demand_forecast_test")
    #this is to reference the UDF creation 
    demand_forecast_udf = dbt.ref("model_training")
    feature_cols = train_df.drop("quantity").columns

    # Function to load the model from file and cache the result
    @cachetools.cached(cache={})
    def load_model(filename):
        
        # Import packages
        import sys
        import os
        import joblib
        
        # Get the import directory where the model file is stored
        import_dir = sys._xoptions.get("snowflake_import_directory")
        
        # Load and return the model
        if import_dir:
            with open(os.path.join(import_dir, filename), 'rb') as file:
                m = joblib.load(file)
                return m

    # Function to predict demand
    def xgboost_predict_demand(X: pd.DataFrame) -> pd.Series:
        
        # Load the model
        model = load_model("xgboost_demand_forecast_model.sav")
        
        # Assign column names to daa
        X.columns = feature_cols
        
        # Get feature order from model
        features = model.get_booster().feature_names
        
        # Get predictions
        predictions = pd.Series(model.predict(X[features])).astype('float64').round(2)
        predictions[predictions < 0] = 0

        return predictions


    session.udf.register(
        func=xgboost_predict_demand,
        name="udf_xgboost_predict_demand",
        stage_location="@FORECAST_STAGE",
        input_types=[T.FloatType()] * len(feature_cols),
        return_type=T.FloatType(),
        replace=True,
        is_permanent=True,
        imports=["@FORECAST_STAGE/xgboost_demand_forecast_model.sav"],
        packages=["xgboost", "joblib", "cachetools"]
    )

    model_name = "xgboost_demand_forecast_model.sav"

    # Train evalutation RMSE
    rmse_train = train_df.select(F.sqrt(F.mean((F.col("quantity") 
                                                    - F.call_udf("udf_xgboost_predict_demand",
                                                                    [F.col(c) for c in feature_cols]))**2)))



    # Test evaluation RMSE
    rmse_test = test_df.select(F.sqrt(F.mean((F.col("quantity") 
                                                    - F.call_udf("udf_xgboost_predict_demand",
                                                                    [F.col(c) for c in feature_cols]))**2)))



    eval_df = pd.json_normalize({"model_name": model_name,
                        "train_ts": session.sql('select current_timestamp()').collect()[0][0],
                        "rmse_train": round(rmse_train.collect()[0][0]),
                        "rmse_test":round(rmse_test.collect()[0][0])})


    return eval_df