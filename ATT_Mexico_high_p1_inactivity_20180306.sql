/** find out all ATT Mexico signups cancellation reason **/
create table fduan.att_mexico_pi_signup as
(
select base.account_id
, base.subscrn_id
, base.signup_mop_partner_name
, base.acquisition_channel
, base.signup_dateint
, d.cancellation_reason_desc
, d.subscrn_status ---OPEN, still acitve, CLOSED , churn
, max(d.latest_billing_period_nbr) as latest_billing_period_nbr
, max(d.billing_end_date) as last_billing_end_date

from dse.ptr_subscrn_signup_retention_up_sum base
left join dse.billing_subscrn_d d
    on base.account_id = d.account_id
    and base.subscrn_id = d.subscrn_id
    
where base.signup_mop_partner_name = 'ATT_MEXICO'
and base.acquisition_channel = 'Browser' -- exclude pre-load
and base.signup_dateint >= 20180604 -- date when we implemente balance check for prepaid customer
and d.billing_start_date>=20180604
group by 1,2,3,4,5,6,7
)
---17,987


/** get viewing inactivity **/
create table fduan.att_mexico_inactivity as
	(select base.*
		,view.fraud_flag
		,view.view_secs
		,case when view.view_secs=0 then 1 else 0 end as P1_inactivity_t_f

		from fduan.att_mexico_pi_signup base
		left join  
			(select account_id, subscrn_id,fraud_flag
				,sum(device_billing_period_view_secs) as view_secs
			from
			dse.playback_subscrn_ptr_device_activity_up_sum 
			where country_desc = 'Mexico'
			and signup_mop_partner_name = 'ATT_MEXICO'
			and signup_acquisition_channel = 'Browser'
			and billing_period_nbr = 1
			and billing_period_end_date>=20180604
			group by 1,2,3
			)view
		on base.account_id = view.account_id
		and base.subscrn_id = view.subscrn_id
	)

--- These customers paid for one month but didn't stream anything from dse.playback_session_f table
select * from fduan.att_mexico_inactivity
where nf_datediff(signup_dateint,last_billing_end_date) = 31
limit 10;


select 
case when cancellation_reason_desc in ('VOL_PARTNER','VOL_CUSTOMER') then 'vol_cancel'
	 when cancellation_reason_desc in ('INVOL_UNPAID','INVOL_PARTNER','INVOL_NO_MOP','INVOL_FRAUD_SUSPICION') then 'invol_cancel'
	 else 'open' end as subscrn_status

	,nf_datediff(signup_dateint,last_billing_end_date) as cancel_days_since_signup
	,P1_inactivity_t_f
	,count(*)

from fduan.att_mexico_inactivity
group by 1,2,3;



/** inactivity trend matches with early cancellation trend **/
select signup_dateint
, count(*) as num_signups
, sum(case when nf_datediff(signup_dateint,last_billing_end_date)<=7 then 1 else 0 end) as num_cancel_within_7_days
from fduan.att_mexico_inactivity
group by 1;

/** distribution of days_in_subscrn for p1 inactivity **/
select case when nf_datediff(signup_dateint,last_billing_end_date) <= 31
then nf_datediff(signup_dateint,last_billing_end_date)
else 999 end as days_in_subscrn
,count(*) as total_signups
,sum(P1_inactivity_t_f) as sum_p1_inactivity
from fduan.att_mexico_inactivity
where signup_dateint between 20180712 and 20181205
group by 1;



/** cancel reason for those who cancelled on the same day of signup **/
select cancellation_reason_desc
,count(*) as total_signups
from fduan.att_mexico_inactivity
where signup_dateint between 20180712 and 20181205
and nf_datediff(signup_dateint,last_billing_end_date)=0
group by 1;
