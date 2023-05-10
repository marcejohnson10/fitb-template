{{
    config(
        materialized ='view'
    )
}}


SELECT CONT_ID,
CASE WHEN (ORGANIZATION_NAME IS NOT NULL ) THEN '1' ELSE '0' END AS Legal_Nm_Populated,
len(ORGANIZATION_NAME) AS Legal_Nm_Len,
POSITION(' ' IN ORGANIZATION_NAME) AS Legal_Nm_Space_Position
FROM {{ create_stream (ref('PDP_ORG_NM_DIM')) }} 
WHERE NAME_USAGE_TYPE=1000003 and Z_CURRENT_FLAG='Y' and METADATA$ACTION = 'INSERT'
order by cont_id
UNION ALL
SELECT CONT_ID,
'0' AS Legal_Nm_Populated,
0 AS Legal_Nm_Len,
0 AS Legal_Nm_Space_Position
FROM {{ ref('PDP_ORG_NM_DIM') }} 
WHERE 1=0
