-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into dbt_demo_project.transform.orders_rslt as DBT_INTERNAL_DEST
        using dbt_demo_project.transform.orders_rslt__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.I_ORDER_ID = DBT_INTERNAL_DEST.I_ORDER_ID
            )

    
    when matched then update set
        "INGESTION_UNIQUE_KEY" = DBT_INTERNAL_SOURCE."INGESTION_UNIQUE_KEY","INGESTION_TYPE" = DBT_INTERNAL_SOURCE."INGESTION_TYPE","INGESTION_METADATA" = DBT_INTERNAL_SOURCE."INGESTION_METADATA","INGESTION_TIMESTAMP" = DBT_INTERNAL_SOURCE."INGESTION_TIMESTAMP","I_ORDER_ID" = DBT_INTERNAL_SOURCE."I_ORDER_ID","T_ORDER_STATUS" = DBT_INTERNAL_SOURCE."T_ORDER_STATUS","T_ORDER_SUMMARY" = DBT_INTERNAL_SOURCE."T_ORDER_SUMMARY"
    

    when not matched then insert
        ("INGESTION_UNIQUE_KEY", "INGESTION_TYPE", "INGESTION_METADATA", "INGESTION_TIMESTAMP", "I_ORDER_ID", "T_ORDER_STATUS", "T_ORDER_SUMMARY")
    values
        ("INGESTION_UNIQUE_KEY", "INGESTION_TYPE", "INGESTION_METADATA", "INGESTION_TIMESTAMP", "I_ORDER_ID", "T_ORDER_STATUS", "T_ORDER_SUMMARY")

;
    commit;