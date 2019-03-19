/*** High P1 invol churn starting December 2018
https://partnerecosystem.prod.netflix.net/retention?store=7abb716ed8696be9fe0bb40eebb4052a
***/

drop table fduan.TEF_CL_rejoin_ptr;
create table fduan.TEF_CL_rejoin_ptr as
	(select ptr.account_id
		,pai.billing_partner_handle as pai
		,ptr.subscrn_id
		,ptr.signup_date
		,ptr.is_rejoin
		,ptr.p1_possible_complete_cnt
		,ptr.p1_invol_cancel_cnt
		,ptr.p1_vol_cancel_cnt
	from dse.ptr_subscrn_signup_retention_up_sum ptr
		left join 
		(select distinct account_id, billing_partner_handle
			from dsecp.subscrn_srvc_signup_partitioned_f pai
			where processing_utc_date >=20181001)pai
		on ptr.account_id = pai.account_id

	where ptr.partner_name = 'Telefonica Chile'
	and ptr.signup_date >= 20181001
		)

/** confirm the increasing P1 invol trend for rejoin **/
select nf_datetrunc('month', signup_date) as signup_month
,is_rejoin
,count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as num_signup
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end)) as num_invol
,count(distinct (case when p1_invol_cancel_cnt=1 then subscrn_id end))*1.0/count(distinct (case when p1_possible_complete_cnt=1 then subscrn_id end)) as invol_rate
from fduan.TEF_CL_rejoin_ptr
group by 1,2;

/** Examples **/
select pai
, account_id
, count(distinct subscrn_id) as num_subs
from fduan.TEF_CL_rejoin_ptr
where is_rejoin = 1
group by 1,2
order by 3 desc
limit 20


select signup_date
from fduan.TEF_CL_rejoin_ptr
where account_id = 288425923712614977


