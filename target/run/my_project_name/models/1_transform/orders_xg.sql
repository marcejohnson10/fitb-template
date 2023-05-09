
  
    

        create or replace  table dbt_demo_project.transform.orders_xg  as
        (select * from DBT_DEMO_PROJECT.raw.orders
        );
      
  