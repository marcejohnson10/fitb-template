
select org_type_code, current_org_flag, count(cont_id)*4 as count_orgs
from {{ ref('org_dim_rslt') }}
group by 1, 2