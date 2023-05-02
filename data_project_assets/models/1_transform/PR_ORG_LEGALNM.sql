{{
    config(
        materialized ='table'
    )
}}
SELECT CONT_ID,
CASE WHEN (ORGANIZATION_NAME IS NOT NULL ) THEN '1' ELSE '0' END AS Legal_Nm_Populated,
len(ORGANIZATION_NAME) AS Legal_Nm_Len,
POSITION(' ' IN ORGANIZATION_NAME) AS Legal_Nm_Space_Position
FROM {{ ref('PDP_ORG_NM_DIM') }} 
WHERE NAME_USAGE_TYPE=1000003 and Z_CURRENT_FLAG='Y'
order by cont_id