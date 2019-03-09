
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
  where
    signup_date >= 20180101
      and funnel_event_desc IN ('signup')
      and de_country_desc='US'
      and billing_partner_desc = 'COMCAST'
      and event_region_date >= 20171231
  group by signup_date,
    de_country_desc,
    gs.subdivision_desc,
    s.billing_partner_desc,
    s.signup_subscription_type
) t1
group by 1, 2
order by 1, 2
;
