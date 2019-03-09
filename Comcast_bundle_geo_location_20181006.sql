--- Below script produce no data


select
  signup_date,
  case when subdivision_desc in ('Connecticut', 'Vermont', 'Virginia', 'Ohio', 'New Hampshire', 'Rhode Island', 'New Jersey', 'District Of Columbia', 'Maryland', 'Maine', 'Massachusetts', 'Pennsylvania', 'New York', 'North Carolina', 'West Virginia', 'Delaware') then 'Northeast'
    when subdivision_desc in ('Oregon','Nevada','Texas','Kansas','Minnesota','Arizona','Colorado','Utah','Missouri','Oklahoma','New Mexico','Nebraska','California','Washington','Alaska', 'Hawaii', 'Idaho', 'Iowa', 'Montana', 'North Dakota', 'South Dakota', 'Wisconsin', 'Wyoming') then 'West'
    else 'Other' end as comcast_region,
  sum(signup_count) as signup_count
from (
  select signup_date,
         de_country_desc as country_iso_code,
         gs.subdivision_desc as subdivision_desc,
         s.billing_partner_desc,
         s.signup_subscription_type,
         SUM(signup_cnt) as signup_count
  from dse.nonmember_regional_geo_funnel_f nm
    join dse.geo_subdivision_d gs
      on nm.de_country_desc=gs.country_iso_code and nm.de_region_desc=gs.subdivision_iso_code
    join dse.subscrn_signup_f sf
      on nm.account_id = sf.account_id and nm.subscrn_id=sf.subscrn_id
    join dse.subscrn_d s
      on nm.account_id=s.account_id and nm.subscrn_id=s.subscrn_id
      and s.signup_date >= 20180701
      and s.country_iso_code = 'US'
      and coalesce(s.fraud_flag, 0) = 0
      and s.is_tester = 0
      and s.signup_subscription_type = 'P'
      and s.signup_billing_partner_desc = 'COMCAST'
      and s.current_connected_to_account_id is not null -- activated bundle

  where
    signup_date >= 20180101
      and funnel_event_desc IN ('signup')
      and de_country_desc='US'
      and billing_partner_desc = 'COMCAST'
      and event_region_date >= 20180630
  group by signup_date,
    de_country_desc,
    gs.subdivision_desc,
    s.billing_partner_desc,
    s.signup_subscription_type
) t1
group by 1, 2
order by 1, 2
;


-- Here is another query that identifies bundle enrollments from Comcast.

select *
from dse.subscrn_d
where
    signup_date >= 20180101
    and country_iso_code = 'US'
    and coalesce(fraud_flag, 0) = 0
    and is_tester = 0
    and signup_subscription_type = 'P'
    and signup_billing_partner_desc = 'COMCAST'
    and current_connected_to_account_id is not null -- activated bundle
limit 20
;


select billing_partner_desc
signup_subscription_type='P' bundle sign up
signup_subscription_type='PS' paying and streaming 
signup_subscription_type='S' streaming


current_connected_to_account_id not null is activated bundle


--------------- Fang's code
--------------- All table in s3://netflix-dataoven-prod-users/hive/warehouse/fduan.db 
--------------- Fang's own location on S3: s3://netflix-dataoven-prod-users/fduan 

create table fduan.comcast_subsrb_d_activations as (
  select account_id
  ,subscrn_id
  ,signup_date
  ,current_connected_to_account_id
  from dse.subscrn_d s
  where s.signup_date >= ${start_date} ----20180401
      and s.country_iso_code = 'US'
      and coalesce(s.fraud_flag, 0) = 0
      and s.is_tester = 0
      and s.signup_subscription_type = 'P'
      and s.signup_billing_partner_desc = 'COMCAST'
      and s.current_connected_to_account_id is not null 
) ---204598



create table fduan.comcast_nm_geo as
  (
  select nm.account_id
  ,nm.subscrn_id
  ,nm.de_country_desc as country_iso_code
  ,nm.de_region_desc
  ,gs.subdivision_desc as subdivision_desc
  ,case when gs.subdivision_desc in ('Connecticut', 'Vermont', 'Virginia', 'Ohio', 'New Hampshire', 'Rhode Island', 'New Jersey', 'District Of Columbia', 'Maryland', 'Maine', 'Massachusetts', 'Pennsylvania', 'New York', 'North Carolina', 'West Virginia', 'Delaware') then 'Northeast'
    when gs.subdivision_desc in ('OregCenon','Nevada','Texas','Kansas','Minnesota','Arizona','Colorado','Utah','Missouri','Oklahoma','New Mexico','Nebraska','California','Washington','Alaska', 'Hawaii', 'Idaho', 'Iowa', 'Montana', 'North Dakota', 'South Dakota', 'Wisconsin', 'Wyoming') then 'West'
    else 'Other' end as comcast_region 
  from dse.nonmember_regional_geo_funnel_f nm
    inner join dse.geo_subdivision_d gs
    on nm.de_country_desc=gs.country_iso_code and nm.de_region_desc=gs.subdivision_iso_code
  inner join  fduan.comcast_subsrb_d_activations ss

    on ss.current_connected_to_account_id = nm.account_id
  where nm.event_region_date >=${geo_start_date} ----20180331
    and nm.funnel_event_desc IN ('signup')
    and nm.de_country_desc='US'
) ---116885

create table fduan.comcast_nm_geo_su_cnt as
(
  select * from 
    (select  
    s.account_id
    ,s.subscrn_id
    ,s.signup_date
    ,s.current_connected_to_account_id

    ,nm.country_iso_code
    ,nm.subdivision_desc
    ,nm.de_region_desc
    ,nm.comcast_region

    ,geo.prim_subdivision_iso_code

    ,row_number() OVER (partition by s.account_id, s.subscrn_id) as record_cnt
    from fduan.comcast_subsrb_d_activations s
    left join fduan.comcast_nm_geo nm
      on nm.account_id=s.current_connected_to_account_id


    left join dse.account_geo_d geo
      on s.current_connected_to_account_id = geo.account_id
      and geo.prim_country_iso_code ='US'
    )
where record_cnt=1
) ---204598

create table fduan.comcast_nm_geo_su_cnt_rpt as
(select * from
  (
  select base.* 
    ,rpt.geo_desc
    ,row_number() OVER (partition by base.account_id, base.subscrn_id) as rec_cnt
  from fduan.comcast_nm_geo_su_cnt base
  left join rpt.ptr_nio_cohort_d rpt
    on base.current_connected_to_account_id = rpt.current_connected_to_account_id
    and base.signup_date = rpt.enrollment_date
  )
where rec_cnt=1
) ---204598

select signup_date
  ,coalesce(coalesce(subdivision_desc,prim_subdivision_iso_code),geo_desc) as state
  ,count(distinct account_id) as num_signup
  from fduan.comcast_nm_geo_su_cnt_rpt
  where signup_date < 20180701
  group by 1,2;


-------------------------------------------------------------------------------
--------------- End of Fang's code that produce the final results -------------
-------------------------------------------------------------------------------















select
  signup_date,
  case when subdivision_desc in ('Connecticut', 'Vermont', 'Virginia', 'Ohio', 'New Hampshire', 'Rhode Island', 'New Jersey', 'District Of Columbia', 'Maryland', 'Maine', 'Massachusetts', 'Pennsylvania', 'New York', 'North Carolina', 'West Virginia', 'Delaware') then 'Northeast'
    when subdivision_desc in ('Oregon','Nevada','Texas','Kansas','Minnesota','Arizona','Colorado','Utah','Missouri','Oklahoma','New Mexico','Nebraska','California','Washington','Alaska', 'Hawaii', 'Idaho', 'Iowa', 'Montana', 'North Dakota', 'South Dakota', 'Wisconsin', 'Wyoming') then 'West'
    else 'Other' end as comcast_region,
  sum(signup_count) as signup_count
from (
  select signup_date,
         de_country_desc as country_iso_code,
         gs.subdivision_desc as subdivision_desc,
         s.billing_partner_desc,
         s.signup_subscription_type,
         SUM(signup_cnt) as signup_count
  from dse.nonmember_regional_geo_funnel_f nm
    join dse.geo_subdivision_d gs
      on nm.de_country_desc=gs.country_iso_code and nm.de_region_desc=gs.subdivision_iso_code
    join dse.subscrn_signup_f sf
      on nm.account_id = sf.account_id and nm.subscrn_id=sf.subscrn_id
    join dse.subscrn_d s
      on nm.account_id=s.account_id and nm.subscrn_id=s.subscrn_id
      and s.signup_date >= 20180701
      and s.country_iso_code = 'US'
      and coalesce(s.fraud_flag, 0) = 0
      and s.is_tester = 0
      and s.signup_subscription_type = 'P'
      and s.signup_billing_partner_desc = 'COMCAST'
      and s.current_connected_to_account_id is not null -- activated bundle

  where
    signup_date >= 20180701
      and funnel_event_desc IN ('signup')
      and de_country_desc='US'
      and billing_partner_desc = 'COMCAST'
      and event_region_date >= 20180630
  group by signup_date,
    de_country_desc,
    gs.subdivision_desc,
    s.billing_partner_desc,
    s.signup_subscription_type
) t1
group by 1, 2
order by 1, 2
;



/*** SQL ****/

with comcast_subsrb_d as (
  select account_id
  ,subscrn_id
  ,signup_date
  ,current_connected_to_account_id
  from dse.subscrn_d s
  where s.signup_date >= ${start_date}
      and s.country_iso_code = 'US'
      and coalesce(s.fraud_flag, 0) = 0
      and s.is_tester = 0
      and s.signup_subscription_type = 'P'
      and s.signup_billing_partner_desc = 'COMCAST'
      and s.current_connected_to_account_id is not null 
),

comcast_nm_geo as(
  select nm.account_id
  ,nm.subscrn_id
  ,nm.de_country_desc as country_iso_code
  ,nm.de_region_desc
  ,gs.subdivision_desc as subdivision_desc
  ,case when gs.subdivision_desc in ('Connecticut', 'Vermont', 'Virginia', 'Ohio', 'New Hampshire', 'Rhode Island', 'New Jersey', 'District Of Columbia', 'Maryland', 'Maine', 'Massachusetts', 'Pennsylvania', 'New York', 'North Carolina', 'West Virginia', 'Delaware') then 'Northeast'
    when gs.subdivision_desc in ('Oregon','Nevada','Texas','Kansas','Minnesota','Arizona','Colorado','Utah','Missouri','Oklahoma','New Mexico','Nebraska','California','Washington','Alaska', 'Hawaii', 'Idaho', 'Iowa', 'Montana', 'North Dakota', 'South Dakota', 'Wisconsin', 'Wyoming') then 'West'
    else 'Other' end as comcast_region 
  from dse.nonmember_regional_geo_funnel_f nm
    inner join dse.geo_subdivision_d gs
    on nm.de_country_desc=gs.country_iso_code and nm.de_region_desc=gs.subdivision_iso_code
 
  where nm.event_region_date >=${geo_start_date}
    and nm.funnel_event_desc IN ('signup')
    and nm.de_country_desc='US'
), 

comcast_nm_geo_su_cnt as (
select nm.account_id
  ,nm.subscrn_id
  ,nm.country_iso_code
  ,nm.subdivision_desc
  ,nm.comcast_region 
  ,s.signup_date
  ,s.current_connected_to_account_id
  ,geo.prim_subdivision_iso_code
  from comcast_subsrb_d s
  left join comcast_nm_geo nm
    on nm.account_id=s.current_connected_to_account_id
  left join dse.account_geo_d geo
    on s.current_connected_to_account_id = geo.account_id
    and geo.prim_country_iso_code ='US'

)


### Check which field is missing more
create table fduan.comcast_bundle_geo as
  ( select ss.account_id
    ,ss.subscrn_id
    ,ss.signup_date
    ,ss.current_connected_to_account_id
    ,nmm.subscrn_id as current_connected_to_subscrn_id
    ,nmm.country_iso_code
    ,nmm.subdivision_desc
    ,nmm.comcast_region 
    ,geo.prim_subdivision_iso_code
    ,rpt.geo_desc
  from 
    (select account_id
    ,subscrn_id
    ,signup_date
    ,current_connected_to_account_id
    from dse.subscrn_d s
    where s.signup_date >= ${start_date}
        and s.country_iso_code = 'US'
        and coalesce(s.fraud_flag, 0) = 0
        and s.is_tester = 0
        and s.signup_subscription_type = 'P'
        and s.signup_billing_partner_desc = 'COMCAST'
        and s.current_connected_to_account_id is not null 
        ) ss
    

  left join
  (select nm.account_id
    ,nm.subscrn_id
    ,nm.de_country_desc as country_iso_code
    ,nm.de_region_desc
    ,gs.subdivision_desc as subdivision_desc
    ,case when gs.subdivision_desc in ('Connecticut', 'Vermont', 'Virginia', 'Ohio', 'New Hampshire', 'Rhode Island', 'New Jersey', 'District Of Columbia', 'Maryland', 'Maine', 'Massachusetts', 'Pennsylvania', 'New York', 'North Carolina', 'West Virginia', 'Delaware') then 'Northeast'
      when gs.subdivision_desc in ('Oregon','Nevada','Texas','Kansas','Minnesota','Arizona','Colorado','Utah','Missouri','Oklahoma','New Mexico','Nebraska','California','Washington','Alaska', 'Hawaii', 'Idaho', 'Iowa', 'Montana', 'North Dakota', 'South Dakota', 'Wisconsin', 'Wyoming') then 'West'
      else 'Other' end as comcast_region 
    from dse.nonmember_regional_geo_funnel_f nm
      inner join dse.geo_subdivision_d gs
      on nm.de_country_desc=gs.country_iso_code and nm.de_region_desc=gs.subdivision_iso_code
   
      where nm.event_region_date >=${geo_start_date}
      and nm.funnel_event_desc IN ('signup')
      and nm.de_country_desc='US'
    )nmm
  
  on ss.current_connected_to_account_id = nmm.account_id

  left join dse.account_geo_d geo
    on ss.current_connected_to_account_id = geo.account_id
    and geo.prim_country_iso_code ='US'


  left join rpt.ptr_nio_cohort_d rpt
    on ss.current_connected_to_account_id = rpt.current_connected_to_account_id

   )


select case when prim_subdivision_iso_code is null then 1 else 0 end as geo_acct_missing_t_f
, case when comcast_region is null then 1 else 0 end as nm_geo_missing_t_f
,case when geo_desc is null then 1 else 0 end as rpt_missing_t_f
, count(*) from fduan.comcast_bundle_geo
group by 1,2,3;


with state_info as(
select signup_date
  ,coalesce(coalesce(subdivision_desc,prim_subdivision_iso_code),geo_desc) as state
  ,count(*) as num_signup
  from fduan.comcast_bundle_geo
  where signup_date < 20180701
  group by 1,2
)

select signup_date
,case when (state = "Baltimore, MD" or state in ("Boston et al, MA-NH","Connecticut","CT","DC","DE","Delaware","District Of Columbia","Harrisburg et al, PA","Hartford & New Haven, CT","MA","Maine","Maryland","Massachusetts","MB","MD","ME","New Hampshire","New Jersey","New York","New York, NY","NH","NJ","NY","OH","Ohio","PA","Pennsylvania","Philadelphia, PA","Pittsburgh, PA","Providence et al, RI-MA","Rhode Island","RI","Richmond-Petersburg, VA","Salisbury, MD","Springfield-Holyoke, MA","VA","Vermont","Virginia","VT","Washington et al, DC-MD","Wilkes Barre et al, PA")) then NorthEast
      when state in ("Atlanta, GA","Augusta-Aiken, GA-SC","Charleston, SC","FL","Florida","Ft. Myers-Naples, FL","GA","Georgia","Jacksonville, FL","Miami-Ft. Lauderdale, FL","Mobile et al, AL-FL","NC","North Carolina","Orlando et al, FL","Savannah, GA","SC","South Carolina","Tampa et al, FL","W. Palm Beach et al, FL") then SouthEast
      when state in ("AL","Alabama","AR","Arizona","Arkansas","AZ","Birmingham et al, AL","Chattanooga, TN","Chicago, IL","CO","Colorado","Detroit, MI","Huntsville et al, AL","IA","ID","IL","Illinois","IN","Indiana","Indianapolis, IN","Iowa","Kansas","Kentucky","Knoxville, TN","KS","KY","LA","LAL","Lansing, MI","Little Rock et al, AR","Louisiana","LUN","Memphis, TN","MI","Michigan","Minnesota","Mississippi","Missouri","MN","MO","Montana","MS","MT","NA","Nashville, TN","ND","NE","Nebraska","Nevada","New Mexico","NM","NP","NV","OK","Oklahoma","SD","South Dakota","Tennessee","Texas","TN","TNN","TX","UT","Utah","West Virginia","Wheeling et al, WV-OH","WI","Wisconsin","WSA","WV","WY","Wyoming") then Central
      when state in ("AK","Alaska","CA","California","Hawaii","HI","OR","Oregon","WA","Washington") then West
      else missing end as region
,case when state = 
,sum(coalesce(num_signup,0)) as signup_cnt
from state_info
group by 1,2;




select signup_date
,case when state in ("Baltimore, MD","Boston et al, MA-NH","Connecticut","CT","DC","DE","Delaware","District Of Columbia","Harrisburg et al, PA","Hartford & New Haven, CT","MA","Maine","Maryland","Massachusetts","MB","MD","ME","New Hampshire","New Jersey","New York","New York, NY","NH","NJ","NY","OH","Ohio","PA","Pennsylvania","Philadelphia, PA","Pittsburgh, PA","Providence et al, RI-MA","Rhode Island","RI","Richmond-Petersburg, VA","Salisbury, MD","Springfield-Holyoke, MA","VA","Vermont","Virginia","VT","Washington et al, DC-MD","Wilkes Barre et al, PA") then NorthEast
      when state in ("Atlanta, GA","Augusta-Aiken, GA-SC","Charleston, SC","FL","Florida","Ft. Myers-Naples, FL","GA","Georgia","Jacksonville, FL","Miami-Ft. Lauderdale, FL","Mobile et al, AL-FL","NC","North Carolina","Orlando et al, FL","Savannah, GA","SC","South Carolina","Tampa et al, FL","W. Palm Beach et al, FL") then SouthEast
      when state in ("AL","Alabama","AR","Arizona","Arkansas","AZ","Birmingham et al, AL","Chattanooga, TN","Chicago, IL","CO","Colorado","Detroit, MI","Huntsville et al, AL","IA","ID","IL","Illinois","IN","Indiana","Indianapolis, IN","Iowa","Kansas","Kentucky","Knoxville, TN","KS","KY","LA","LAL","Lansing, MI","Little Rock et al, AR","Louisiana","LUN","Memphis, TN","MI","Michigan","Minnesota","Mississippi","Missouri","MN","MO","Montana","MS","MT","NA","Nashville, TN","ND","NE","Nebraska","Nevada","New Mexico","NM","NP","NV","OK","Oklahoma","SD","South Dakota","Tennessee","Texas","TN","TNN","TX","UT","Utah","West Virginia","Wheeling et al, WV-OH","WI","Wisconsin","WSA","WV","WY","Wyoming") then Central
      when state in ("AK","Alaska","CA","California","Hawaii","HI","OR","Oregon","WA","Washington") then West
      else missing end as region
,sum(num_signup) as signup_cnt
from 
  (select signup_date
  ,coalesce(coalesce(subdivision_desc,prim_subdivision_iso_code),geo_desc) as state
  ,count(distinct account_id) as num_signup
  from fduan.comcast_bundle_geo
  where signup_date < 20180701
  group by 1,2) a
group by 1,2;



select signup_date, count(*) 
from rpt.ptr_nio_cohort_d
where signup_subscription_type = 'PS'
and  nio_partner_name= 'COMCAST'
and enrollment_date = 20180520
group by 1;





--------------- Fang's code for Comcast invol cancel calculation
--------------- All table in s3://netflix-dataoven-prod-users/hive/warehouse/fduan.db 
--------------- Fang's own location on S3: s3://netflix-dataoven-prod-users/fduan 

--possible_complete_cnt as denominator for invol/vol cancel calculation
--vol_cancel_cnt as vol cancel cnt
--invol_cancel_cnt as invol cancel cnt
--Comcast payment integration, paying account created when user enroll, Comcast start to pay us when user enroll
--When user activate the account, a streaming account is linked to the paying account
--vol/invol cancel can only come from Comcast side, since Netflix don't charge individual consumer
--Comcast will send a batch file saying these customers(with PAI) no longer subscribe to Netflix
--Once Comcast side said vol/invol cancel, paying account will be marked as end, while streaming account will be onhold for 30 days. untill customers provide their own MOP to continue subscription


select 
billing_period_end_date
,cancellation_reason_desc
,final_try_pmt_method_desc
,sum(possible_complete_cnt) as denominator
,sum(vol_cancel_cnt) as num_vol_cancel
,sum(invol_cancel_cnt) as num_invol_cancel

from dse.billing_period_end_f
where billing_period_nbr=2
and billing_partner_desc='COMCAST'
and final_try_pmt_type_desc='COMCAST'
and coalesce(fraud_flag,0) = 0
and country_iso_code = 'US'
and billing_period_end_date>=20180101
group by 1,2,3;



