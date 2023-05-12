{{
    config(
        materialized ='table'
    )
}}

SELECT CONT_ID,
CASE WHEN (ORGANIZATION_NAME IS NOT NULL ) THEN '1' ELSE '0' END AS Legal_Full_Nm_Populated,
len(ORGANIZATION_NAME) AS Legal_Full_Nm_Len,
POSITION(' ' IN ORGANIZATION_NAME) AS Legal_Full_Nm_Space_Positionx
FROM {{ ref('PDP_ORG_NM_DIM') }}
WHERE NAME_USAGE_TYPE=1000004 and Z_CURRENT_FLAG<>'N'
order by cont_id