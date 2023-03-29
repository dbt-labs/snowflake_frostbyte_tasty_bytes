import pandas as pd

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["pandas"]
    )

    orders_ref = dbt.ref("orders_v")

    return orders_ref