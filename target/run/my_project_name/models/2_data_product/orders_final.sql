
  create or replace   view dbt_demo_project.data_product.orders_final
  
   as (
    select * from dbt_demo_project.transform.orders_rslt
  );

