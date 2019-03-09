-- how many Comcast bundle activations reach stream limit

with activated_bundle as (
select account_id,current_connected_to_account_id,subscrn_id, signup_plan_id, latest_plan_id, signup_date
-- 3088  --2S
-- 4001  --1S
-- 3108  --4S
from dse.subscrn_d
where
    signup_date >= 20180401
    and country_iso_code = 'US'
    -- and coalesce(fraud_flag, 0) = 0
    -- and is_tester = 0
    and signup_subscription_type = 'P'
    and signup_billing_partner_desc = 'COMCAST'
    and current_connected_to_account_id is not null -- activated bundle
),

accts_exceed_limit as (
select distinct customer_id
from vault.conc_stream_block_f
where country_iso_code = 'US'
and event_utc_date >=20180401
),

accts_limits as (
select signup_plan_id,latest_plan_id
,nf_datetrunc('month',signup_date) as signup_month
,count(distinct subscrn_id) as num_activations
,count(distinct aa.customer_id) as num_activate_exceed
from activated_bundle bn
left join accts_exceed_limit aa
on bn.current_connected_to_account_id = aa.customer_id
group by 1,2,3
)

select * from accts_limits;

-- device id check for 4K device


with activated_bundle as (
select account_id,current_connected_to_account_id,subscrn_id, latest_plan_id, signup_date
-- 3088  --2S
-- 4001  --1S
-- 3108  --4S
from dse.subscrn_d
where
    signup_date >= 20180401
    and country_iso_code = 'US'
    and coalesce(fraud_flag, 0) = 0
    and is_tester = 0
    and signup_subscription_type = 'P'
    and signup_billing_partner_desc = 'COMCAST'
    and current_connected_to_account_id is not null -- activated bundle
),

ptr_4k as (
select distinct account_id, signup_device_type_extended_name
from dse.ptr_subscrn_signup_retention_up_sum
where signup_device_type_extended_name like '1938%'
and  signup_dateint >=20180401
),

accts_4k as (
select latest_plan_id
,nf_datetrunc('month',signup_date) as signup_month
,count(distinct subscrn_id) as num_activations
,count(distinct aa.account_id) as num_activate_exceed
from activated_bundle bn
left join ptr_4k aa
on bn.current_connected_to_account_id = aa.account_id
group by 1,2
)

select * from accts_4k;
