-- https://pluto.prod.netflix.net/explore/United_States/all/performance/?metric=invol_cancel_cnt&filters=billing_period_grp_p7_plus%3A2%3Bpmt_type%3ACredit%7CDebit%7CPrepaid




select 
-- alloc.is_free_trial_at_signup
-- ,alloc.signup_date
alloc.partner_name
,case when alloc.partner_name = 'Charter' then 'Charter'
		else 'other_US_MVPD' end as partner_name_grp
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
	,sum(p1_possible_complete_cnt) as p1_denominator
	,sum(p1_invol_cancel_cnt) as p1_invol_cancel
	,sum(p1_vol_cancel_cnt) as p1_vol_cancel
	from dse.ptr_subscrn_signup_retention_up_sum alloc
		left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
    	on alloc.account_id = bp1.account_id
    	and alloc.subscrn_id = bp1.subscrn_id

    	inner join (select partner_name, sum(case when mop_type='Netflix' then 1 else 0 end) as netflix_mop
					,sum(case when mop_type<>'Netflix' then 1 else 0 end) as partner_mop
					from dse.ptr_subscrn_signup_retention_up_sum
					where signup_date between 20180801 and 20180930
					and country_desc='United States'
					and signup_hw_category =  'MVPD Set Top Box'
					group by 1) pp---Choose those US Netflix mop only MVPD set top box
    	on alloc.partner_name = pp.partner_name
    	and pp.partner_mop=0

	where alloc.signup_hw_category =  'MVPD Set Top Box'
	and country_desc='United States'
	and alloc.signup_date between 20180801 and 20180930
	group by 1,2,3,4,5





select 
alloc.account_id
,alloc.subscrn_id
,bs.cancellation_reason_desc
from dse.ptr_subscrn_signup_retention_up_sum alloc
inner join dse.billing_subscrn_d bs
	on alloc.account_id = bs.account_id
	and alloc.subscrn_id = bs.subscrn_id
	and bs.billing_end_date between 20180801 and 20181115
where alloc.p1_invol_cancel_cnt=1
and alloc.partner_name = 'Charter'
and alloc.signup_date between 20180801 and 20180930


-- charter only
select 
bs.cancellation_reason_desc

, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
,case when a.subscrn_id is not null then 1 else 0 end as ESN_hold_t_f
,count(*) as cnt_cancel_reason
from dse.ptr_subscrn_signup_retention_up_sum alloc
inner join dse.billing_subscrn_d bs
	on alloc.account_id = bs.account_id
	and alloc.subscrn_id = bs.subscrn_id
	and bs.billing_end_date between 20180801 and 20181115
left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
    	on alloc.account_id = bp1.account_id
    	and alloc.subscrn_id = bp1.subscrn_id    	
left outer join
(
select distinct account_id, subscrn_id from dse.subscrn_on_hold_f where hold_type_id=410 and event_date>=20180701
)a on a.account_id=alloc.account_id and a.subscrn_id=alloc.subscrn_id

where alloc.p1_invol_cancel_cnt=1 
and alloc.partner_name = 'Charter'
and alloc.signup_date between 20180801 and 20180930
group by 1,2,3;




-- other MVPD
select 

bs.cancellation_reason_desc
,alloc.p1_invol_cancel_cnt
,alloc.p1_vol_cancel_cnt
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
,count(*) as cnt_cancel_reason
from dse.ptr_subscrn_signup_retention_up_sum alloc
inner join dse.billing_subscrn_d bs
	on alloc.account_id = bs.account_id
	and alloc.subscrn_id = bs.subscrn_id
	and bs.billing_end_date between 20180801 and 20181115

inner join (select partner_name, sum(case when mop_type='Netflix' then 1 else 0 end) as netflix_mop
					,sum(case when mop_type<>'Netflix' then 1 else 0 end) as partner_mop
					from dse.ptr_subscrn_signup_retention_up_sum
					where signup_date between 20180801 and 20180930
					and country_desc='United States'
					and signup_hw_category =  'MVPD Set Top Box'
					group by 1) pp---Choose those US Netflix mop only MVPD set top box
    	on alloc.partner_name = pp.partner_name
    	and pp.partner_mop=0	
left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
    	on alloc.account_id = bp1.account_id
    	and alloc.subscrn_id = bp1.subscrn_id    	

where (alloc.p1_invol_cancel_cnt=1 or alloc.p1_vol_cancel_cnt=1)
and alloc.signup_date between 20180801 and 20180930
group by 1,2,3,4;


/** geo information for charter **/

create table fduan.charter_invol_geo_city as 
(
select distinct
  ncf.account_id
  ,ncf.p1_possible_complete_cnt
  ,ncf.p1_invol_cancel_cnt
  ,ncf.p1_vol_cancel_cnt
  , d.device_category as connect_device_category
  , d.input_flow as connect_input_flow
  , d.geo_country as connect_geo_country
  , d.geo_region as connect_geo_region
  , d.geo_city as connect_geo_city
  , d.geo_zip as connect_geo_zip
  , d.geo_dma as connect_geo_dma_id
  , dma.dma_desc as connect_geo_dma_desc
  , dma.longitude as connect_geo_longitude
  , dma.latitude as connect_geo_latitude
  , ncf.signup_date as signup_date
from 


(	select account_id
	,max(p1_possible_complete_cnt) as p1_possible_complete_cnt
	,max(p1_invol_cancel_cnt) as p1_invol_cancel_cnt
	,max(p1_vol_cancel_cnt) as p1_vol_cancel_cnt
	,min(signup_date) as signup_date
	from dse.ptr_subscrn_signup_retention_up_sum 
	where partner_name = 'Charter'
			and signup_date between 20180801 and 20180930
			group by 1
		)ncf


left join dse.dynecom_execution_event_f d
  on ncf.account_id = d.input_account_id
  and d.utc_date between 20180801 and 20181001
left join dse.geo_dma_d dma
  on cast(d.geo_dma as BIGINT) = cast(dma.dma_id as BIGINT)
  and d.geo_country = 'US'
where
  ncf.signup_date between 20180801 and 20180930
  and d.utc_date between 20180801 and 20181001
  and (d.execution_id is not null)
)


select connect_geo_region
,count(distinct(case when p1_possible_complete_cnt=1 then account_id end)) as p1_possible_complete
,count(distinct(case when p1_invol_cancel_cnt=1 then account_id end)) as p1_invol_cancel_cnt
,count(distinct(case when p1_vol_cancel_cnt=1 then account_id end)) as p1_vol_cancel_cnt
from fduan.charter_invol_geo_city
group by 1;

degenerate_value_map
txn_type = 'validate' 
txn_type = , 'charge' --txn or mop update on hold

product_code ='T' --1st validation event not MOP update, not account_update



-- drop table fduan.charter_invol_mop_postal_aug;
-- create table fduan.charter_invol_mop_postal_aug as
-- (
-- select 
-- alloc.account_id
-- ,alloc.subscrn_id
-- ,alloc.signup_date
-- ,alloc.partner_name
-- , case when bp1.first_try_pmt_type_desc is null then 'Unknown'
--     when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
--     else 'no_signup'
--     end as signup_pmt_type_desc
--   ,p1_possible_complete_cnt
--   ,p1_invol_cancel_cnt
--   ,p1_vol_cancel_cnt
-- ,peh.mop_postal_code
-- ,peh.txn_mop_postal_code
-- ,peh.postal_code_actual
-- ,peh.event_utc_date
-- ,peh.event_utc_ms_ts
-- ,peh.row_cnt

--   from dse.ptr_subscrn_signup_retention_up_sum alloc
--     left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
--       on alloc.account_id = bp1.account_id
--       and alloc.subscrn_id = bp1.subscrn_id
--     left join (select 
--             account_id
--             ,degenerate_values_map['mop_postal_code'] as mop_postal_code
--             ,degenerate_values_map['txn_mop_postal_code'] as txn_mop_postal_code
--             ,degenerate_values_map['postal_code_actual'] as postal_code_actual
--             -- ,degenerate_values_map['txn_type'] as txn_type 
--             -- ,degenerate_values_map['product_code'] as product_code
--             -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
--             -- ,degenerate_values_map['mop_expiration'] as mop_expiration
--             -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
--             -- ,degenerate_values_map['mop_id'] as mop_id 
--             -- ,degenerate_values_map['pmt_processor'] as pmt_processor
--             -- ,degenerate_values_map['retry_mode'] as retry_mode
--             -- ,degenerate_values_map['cti_card_type'] as cti_card_type
--             -- ,degenerate_values_map['mop_subtype'] as mop_subtype
--             ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
--             ,event_utc_date
--             -- ,event_utc_ts
--             ,event_utc_ms_ts
--             ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
--           from dse.pmt_events_hourly_f
--           where event_utc_date >= 20180801
--           and country_code='US'
--           and degenerate_values_map['mop_postal_code'] is not null
--       )peh
--     on alloc.account_id = peh.account_id
--     and alloc.signup_date <= nf_dateadd(event_utc_date, 1) 
--   where alloc.signup_hw_category =  'MVPD Set Top Box'
--   and alloc.country_desc='United States'
--   and alloc.signup_date between 20180801 and 20180831
--   and alloc.partner_name in ('Altice USA','Verizon','Charter')
--   )







drop table fduan.charter_invol_mop_postal;
create table fduan.charter_invol_mop_postal as
(
select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date between 20180801 and 20181010
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <=event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  

UNION all

select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date between 20180901 and 20181110
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <= event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  


UNION all

select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date between 20181001 and 20181210
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <= event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  


UNION all

select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date between 20181101 and 20190110
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <= event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  


UNION all

select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date between 20181201 and 20190210
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <= event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  



UNION all

select 
alloc.account_id
,alloc.subscrn_id
,alloc.signup_date
,alloc.partner_name
, case when bp1.first_try_pmt_type_desc is null then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
  ,p1_possible_complete_cnt
  ,p1_invol_cancel_cnt
  ,p1_vol_cancel_cnt
,peh.mop_postal_code
,peh.bin_metadata_type
,peh.event_utc_date
,peh.event_utc_ms_ts

  from dse.ptr_subscrn_signup_retention_up_sum alloc
    left join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20180601) bp1
      on alloc.account_id = bp1.account_id
      and alloc.subscrn_id = bp1.subscrn_id
    left join (select 
            account_id
            ,degenerate_values_map['mop_postal_code'] as mop_postal_code
            -- ,degenerate_values_map['txn_type'] as txn_type 
            -- ,degenerate_values_map['product_code'] as product_code
            -- ,degenerate_values_map['mop_expiration_actual'] as mop_expiration_actual
            -- ,degenerate_values_map['mop_expiration'] as mop_expiration
            -- ,degenerate_values_map['bin_metadata_id'] as bin_metadata_id
            -- ,degenerate_values_map['mop_id'] as mop_id 
            -- ,degenerate_values_map['pmt_processor'] as pmt_processor
            -- ,degenerate_values_map['retry_mode'] as retry_mode
            -- ,degenerate_values_map['cti_card_type'] as cti_card_type
            -- ,degenerate_values_map['mop_subtype'] as mop_subtype
            ,degenerate_values_map['bin_metadata_type'] as bin_metadata_type
            ,event_utc_date
            -- ,event_utc_ts
            ,event_utc_ms_ts
            ,ROW_NUMBER() OVER (partition by account_id order by event_utc_ms_ts) as row_cnt
          from dse.pmt_events_hourly_f
          where event_utc_date >= 20190101 
          and country_code='US'
          and degenerate_values_map['mop_postal_code'] is not null
      )peh
    on alloc.account_id = peh.account_id
    and alloc.signup_date between nf_dateadd(event_utc_date, -1) and nf_dateadd(event_utc_date, 2) 
  where alloc.signup_hw_category =  'MVPD Set Top Box'
  and alloc.country_desc='United States'
  and alloc.signup_date <= event_utc_date
  and alloc.partner_name in ('Altice USA','Verizon','Charter')
  

  );




select signup_date
,partner_name
,signup_pmt_type_desc
,mop_postal_code
,count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as num_p1_complete
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end)) as num_p1_invol
,count(distinct (case when p1_vol_cancel_cnt=1 then subscrn_id end)) as num_p1_vol
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end))*1.0/count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as p1_invol_rate
,count(distinct (case when p1_vol_cancel_cnt=1 then subscrn_id end))*1.0/count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as p1_vol_rate
from fduan.charter_invol_mop_postal
group by 1,2,3,4;


select 
nf_timestamp(signup_date) as signup_date
,partner_name
,signup_pmt_type_desc
,trim(substr(base.mop_postal_code, 1, 5)) as mop_postal_code
,d.state_desc
,d.county_desc
,d.cbsa_desc
,count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as num_p1_complete
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end)) as num_p1_invol
,count(distinct (case when p1_vol_cancel_cnt=1 then subscrn_id end)) as num_p1_vol
from fduan.charter_invol_mop_postal base
inner join dse.geo_zip_d d
on cast(trim(substr(base.mop_postal_code, 1, 5)) as varchar) = cast(d.zip_postal_desc as varchar)

where mop_postal_code is not null
and mop_postal_code !='--'
group by 1,2,3,4,5,6,7




select 
partner_name
-- ,signup_pmt_type_desc
,d.state_desc
,count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as num_p1_complete
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end)) as num_p1_invol
,count(distinct (case when p1_vol_cancel_cnt=1 then subscrn_id end)) as num_p1_vol
from fduan.charter_invol_mop_postal base
left join dse.geo_zip_d d
on cast(trim(substr(base.mop_postal_code, 1, 5)) as varchar) = cast(d.zip_postal_desc as varchar)

where mop_postal_code is not null
and mop_postal_code !='--'
group by 1,2;



/** Deprecated geo city based on dynecom IP, not as accurate as mop zip code above **/

create table fduan.charter_invol_geo_city as 
(
select distinct
  ncf.account_id
  ,ncf.p1_possible_complete_cnt
  ,ncf.p1_invol_cancel_cnt
  ,ncf.p1_vol_cancel_cnt
  , d.device_category as connect_device_category
  , d.input_flow as connect_input_flow
  , d.geo_country as connect_geo_country
  , d.geo_region as connect_geo_region
  , d.geo_city as connect_geo_city
  , d.geo_zip as connect_geo_zip
  , d.geo_dma as connect_geo_dma_id
  , dma.dma_desc as connect_geo_dma_desc
  , dma.longitude as connect_geo_longitude
  , dma.latitude as connect_geo_latitude
  , ncf.signup_date as signup_date
from 


(	select account_id
	,max(p1_possible_complete_cnt) as p1_possible_complete_cnt
	,max(p1_invol_cancel_cnt) as p1_invol_cancel_cnt
	,max(p1_vol_cancel_cnt) as p1_vol_cancel_cnt
	,min(signup_date) as signup_date
	from dse.ptr_subscrn_signup_retention_up_sum 
	where partner_name = 'Charter'
			and signup_date between 20180801 and 20180930
			group by 1
		)ncf


left join dse.dynecom_execution_event_f d
  on ncf.account_id = d.input_account_id
  and d.utc_date between 20180801 and 20181001
left join dse.geo_dma_d dma
  on cast(d.geo_dma as BIGINT) = cast(dma.dma_id as BIGINT)
  and d.geo_country = 'US'
where
  ncf.signup_date between 20180801 and 20180930
  and d.utc_date between 20180801 and 20181001
  and (d.execution_id is not null)
)
