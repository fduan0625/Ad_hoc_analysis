/** -- birdbox, show_title_id=80196789 --*/
/** -- Dec 21, 2018 launched           --*/
/** -- @fduan                          --*/
/** -- March 3, 2019                   --*/


-- step 1 : select all signups between 20181221 and 20181228

-- country profile
-- acquisition channel
-- rejoin or not
-- free trial or not
-- signup plan, latest plann 
create table fduan.signups_between_20181221_20181228 as
(select account_id
	,subscrn_id
	,signup_date
	,signup_ts
	,signup_utc_ts
	,signup_billing_partner_desc
	,package_offer_provider_desc
	,is_channel_giftcard
	,is_dcb_signup
	,is_free_trial_at_signup
	,is_mds
	,is_rejoin
	,fraud_flag
	,country_iso_code
	,signup_plan_id
	,latest_plan_id
	,latest_service_stop_date
	,is_voluntary_cancel
	,latest_subscrn_period_nbr
	,signup_device_type_id
from dse.subscrn_d
where signup_date between 20181221 and 20181228
	)

-- 4,424,127



-- step 2 : select all signups between 20181221 and 20181228 that watch BirdBox during the week of 20181221 and 20181228

-- Time since signup
-- did user finish more than 6 minutes
-- did user finish more than 70% of playtime
-- primary device that play BirdBox
-- multiple devices usage 

-- 62,276,751 -- unique accts
-- 54,259,039 -- unique qp accts

-- Joey 45,037,125
create table fduan.Birdbox_watch_20181221_20181228 as
(select country_iso_code
	,region_date
	,play_device_type_id
	,play_device_model
	,account_id
	,profile_id
	,(case when LOWER(a.play_ui_version) LIKE '%darwin%' then 1 else 0 end) as darwin_t_f
	,d.device_type_name
	,d.brand
	,d.mso_partner
	,d.hw_category
	,case when d.hw_category = 'MVPD Set Top Box' then 1 else 0 end as stb_t_f
	,sum(a.standard_sanitized_duration_sec) as view_secs
	,sum(a.session_cnt) as session_cnt


from dse.loc_acct_device_ttl_sum a
	left join dse.device_type_rollup_d d
		on a.play_device_type_id = d.device_type_id
where a.region_date between 20181221 and 20181228
and a.show_title_id = 80196789 --BirdBox
and a.is_test_play=0
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
-- 121716491

-- step 3 : User signup and do not watch birdbox compare to user signup who watch BirdBox
-- p1 retention
-- p2 net realzied revenue
-- p2 plan upgrade


-- unique subscrn_id level
create table fduan.retention_for_signups_between_20181221_20181228 as 
(select base.*
	,max(bpef.billing_period_nbr) as billing_period_nbr
	,max(bpef.vol_cancel_cnt) as vol_cancel_cnt
	,max(bpef.invol_cancel_cnt) as invol_cancel_cnt
	,max(bpef.invol_vol_cancel_cnt) as invol_vol_cancel_cnt
	,max(bpef.onhold_cnt) as onhold_cnt
	,max(bpef.recovered_cnt) as recovered_cnt
	,sum(coalesce(adjusted_revenue_amt_usd,0)) as net_revenue
from fduan.signups_between_20181221_20181228 base
left join dse.billing_period_end_f bpef
	on base.account_id=bpef.account_id
	and base.subscrn_id = bpef.subscrn_id
where bpef.possible_complete_cnt=1
and bpef.billing_period_end_date >=20181221
and bpef.is_tester_account=0
and bpef.billing_period_nbr<=2

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

)
-- 4,345,344

create table fduan.birdbox_main_device as
(select a.*
,ROW_NUMBER() OVER (partition by account_id, hw_category order by view_minutes desc) as device_rank
from
	(
	select account_id
	,hw_category
	,min(region_date) as first_view_date
	,count(distinct profile_id) as num_profiles
	,max(darwin_t_f) as darwin_t_f
	,max(stb_t_f) as stb_t_f
	,sum(view_secs)/60 as view_minutes
	,sum(session_cnt) as total_sessions
	from fduan.birdbox_watch_20181221_20181228
	where view_secs>=360 --- qualified play only
	group by 1,2)a
)

-- 70153161

select 
e.hw_category
,(case when d.account_id is not null then 1 else 0 end) as watch_birdbox_t_f
,count(distinct base.subscrn_id) as num_signups
,count(distinct (case when billing_period_nbr=2 and base.invol_vol_cancel_cnt <>1 then base.subscrn_id end)) as num_retained

from fduan.retention_for_signups_between_20181221_20181228 base
 	left join (select distinct account_id from fduan.birdbox_main_device) d
 	on base.account_id = d.account_id
 	left join (select account_id, hw_category 
 		from fduan.birdbox_main_device
 		where device_rank=1
 		)e
 	on base.account_id = e.account_id
group by 1,2;
