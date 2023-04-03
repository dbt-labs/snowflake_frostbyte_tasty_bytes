import itertools
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window
import pandas as pd

def model(dbt, session):

    dbt.config(
        packages=["pandas"]
    )

    train_df = dbt.ref("demand_forecast_train")

    param_grid = {
        "MAX_DEPTH": [6, 9, 12],
        "LEARNING_RATE": [0.01, 0.03, 0.1],
        "N_ESTIMATORS": [100, 200, 300]
    }

    schema = T.StructType(
        [
            T.StructField("MAX_DEPTH", T.IntegerType()),
            T.StructField("LEARNING_RATE", T.FloatType()),
            T.StructField("N_ESTIMATORS", T.IntegerType()),
        ]
    )

    params_df = session.create_dataframe(
        list(itertools.product(*[param_grid[param] for param in param_grid])),
        schema)


    params_df = params_df.select(
        "*", F.row_number().over(Window.order_by(F.lit(1))).as_("HP_ID")
    )

    features_df = train_df.sample(n=1000).select(F.array_construct('*').alias("feature_vector"))
    feature_columns = train_df.columns
    tuning_df = params_df.crossJoin(features_df)


    schema = T.StructType(
        [
            T.StructField("MAX_DEPTH", T.IntegerType()),
            T.StructField("LEARNING_RATE", T.FloatType()),
            T.StructField("N_ESTIMATORS", T.IntegerType()),
            T.StructField("RMSE", T.FloatType()),
        ]
    )

    @F.udtf(
        output_schema=schema,
        input_types=[
            T.IntegerType(),
            T.FloatType(),
            T.IntegerType(),
            T.ArrayType()
            
        ],
        name="hyperparameter_tuning",
        session=session,
        is_permanent=True,
        stage_location="@forecast_stage",
        packages=["snowflake-snowpark-python", "pandas", "scikit-learn", "xgboost"],
        replace=True,
    )
    class Tuner:
        #Initializes state for stateful processing of input partitions
        def __init__(self):
            self.MAX_DEPTH = None
            self.LEARNING_RATE = None
            self.N_ESTIMATORS = None
            self.feature_vector = []
            self.processedFirstRow = False

        def process(
            self,
            MAX_DEPTH,
            LEARNING_RATE,
            N_ESTIMATORS,
            feature_vector,
        ):
            if not self.processedFirstRow:
                self.MAX_DEPTH = MAX_DEPTH
                self.LEARNING_RATE = LEARNING_RATE
                self.N_ESTIMATORS = N_ESTIMATORS
                self.processedFirstRow = True
            
            self.feature_vector.append([None if f == 'undefined' else f for f in feature_vector])

        def end_partition(self):
            from sklearn.metrics import mean_squared_error
            from sklearn.model_selection import train_test_split
            from xgboost import XGBRegressor
            import pandas as pd
            
            df = pd.DataFrame(
                self.feature_vector,
                columns=feature_columns,
            )
            
            df = df.apply(pd.to_numeric,errors='coerce')

            dfx = df.loc[:, df.columns != "QUANTITY"]
            dfy = df.loc[:, df.columns == "QUANTITY"]

            X_train, X_test, y_train, y_test = train_test_split(
                dfx, dfy, test_size=0.2, random_state=0
            )

            model = XGBRegressor(
                max_depth=self.MAX_DEPTH,
                learning_rate=self.LEARNING_RATE,
                n_estimators=self.N_ESTIMATORS,
                n_jobs=1,
                seed=0,
            )
            
            model.fit(X_train, y_train)

            y_pred = model.predict(X_test)

            yield (
                self.MAX_DEPTH,
                self.LEARNING_RATE,
                self.N_ESTIMATORS,
                mean_squared_error(y_test, y_pred),
            )
            
    return tuning_df