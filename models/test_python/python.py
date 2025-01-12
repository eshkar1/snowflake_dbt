import snowflake.snowpark.function as f

def model(dbt,session):
    df = session.sql("select * from FINANCE_DB.BILLING_SCH.GLOBAL_TENANT_HISTORY_4BILLING_TBL")

    df_filter = df.filter(f.col("ROOT") == 'US-733')
    
return df_filter
