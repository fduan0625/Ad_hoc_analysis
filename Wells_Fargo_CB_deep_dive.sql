---- CHARGEBACKS start from 3/1/2018
with chargeback_dim as 
(
   select
    original_txn_utc_date event_utc_date
    , case when original_txn_type ='SALE' then 'RENEWAL' else 'REJOIN_CHARGE' end txn_type

    , original_txn_pmt_processor processor
    , case when original_txn_type !='SALE' then 'REJOIN_CHARGE' else retry_mode end as retry_mode
    , billing_period_nbr
    , token_used
    , geo.estaff_subregion_desc
    , geo.country_desc
    , upper(coalesce(ig.grouped_pmt_institution_desc, trim(regexp_replace(inst.pmt_institution_desc, '"', '""')))) issuer_institution_desc_grp
    , mop.affiliation pmt_brand
    , mop.pmt_type_desc pmt_type
    , mop.clear_bin bin

    , sum(chargeback_cnt)chargeback_cnt
    from (
        select
          event_utc_date chargeback_event_utc_date
          , country_code
          , degenerate_values_map['original_txn_pmt_processor'] original_txn_pmt_processor
          , natural_key_map['original_txn_id'] original_txn_id
          , degenerate_values_map['acquirer'] pmt_acquirer
          , degenerate_values_map['original_txn_type']original_txn_type
          , cast(degenerate_values_map['billing_period']as int)billing_period_nbr
          , degenerate_values_map['retry_mode']retry_mode
          , cast(date_format(from_unixtime(cast(degenerate_values_map['original_txn_date'] as bigint)/1000), '%Y%m%d')as int)original_txn_utc_date
          , degenerate_values_map['currency_code']currency_code
          , degenerate_values_map['token_used']token_used
          , degenerate_values_map['is_debit_transaction']is_debit_transaction
          , coalesce(degenerate_values_map['is_chained_txn'],'false') is_chained_txn
          , dimension_sk_map['pmt_mop_metadata_sk'] pmt_mop_metadata_sk
          , facts_map['chargeback_amount']/100 as chargeback_local_amount
          , facts_map['record_count'] as chargeback_cnt
      FROM dse.pmt_events_hourly_f
      WHERE
          pmt_event_type = 'chargeback_transaction'
          AND event_utc_date >= ${cb_start_date} 
          AND coalesce(degenerate_values_map['is_duplicate'], 'false') !='true' 
          and degenerate_values_map['original_txn_pmt_processor'] != '--' 
        )chargebacks

 
    join dse.pmt_mop_metadata_d mop
        on chargebacks.pmt_mop_metadata_sk = mop.pmt_mop_metadata_sk
    join dse.pmt_institution_v2_d inst
        on mop.pmt_institution_sk = inst.pmt_institution_sk
    left outer join dse.pmt_institution_grouped_d  ig
        on inst.pmt_institution_desc = ig.pmt_institution_desc
    join dse.geo_country_d geo
        on chargebacks.country_code = geo.country_iso_code
    where original_txn_utc_date >= ${event_start_date}
    group by
    original_txn_utc_date
    , case when original_txn_type ='SALE' then 'RENEWAL' else 'REJOIN_CHARGE' end 

    , original_txn_pmt_processor 
    , case when original_txn_type !='SALE' then 'REJOIN_CHARGE' else retry_mode end
    , billing_period_nbr
    , token_used
    , geo.estaff_subregion_desc
    , geo.country_desc
    , upper(coalesce(ig.grouped_pmt_institution_desc, trim(regexp_replace(inst.pmt_institution_desc, '"', '""'))))
    , mop.affiliation 
    , mop.pmt_type_desc 
    , mop.clear_bin

),

bill2 as 
(
    select
    event_utc_date
    , 'SALE' txn_type
    , processor
    , retry_mode
    , billing_period_nbr
    , token_used
    , estaff_subregion_desc
    , country_desc
    , issuer_institution_desc_grp
    , pmt_brand
    , pmt_type
    , bin
    , sum(approved_cnt)approved_cnt
    from dse.pmt_renewal_txn_agg
    where event_utc_date >= ${event_start_date}
    group by
    event_utc_date
    , 'SALE'
    , processor
    , retry_mode
    , billing_period_nbr
    , token_used
    , estaff_subregion_desc
    , country_desc
    , issuer_institution_desc_grp
    , pmt_brand
    , pmt_type
    , bin
),

bill1 as
( select event_utc_date
    , 'REJOIN_CHARGE' txn_type
    , processor
    , 'REJOIN_CHARGE' retry_mode
    , 1 billing_period_nbr
    , 'false' token_used
    , estaff_subregion_desc
    , country_desc
    , issuer_institution_desc_grp
    , pmt_brand
    , pmt_type
    , bin
    , sum(charge_approved_cnt)approved_cnt
    from dse.pmt_validation_txn_agg
    where event_utc_date >= ${event_start_date}
    group by
    event_utc_date
    , 'REJOIN_CHARGE' 
    , processor
    , 'REJOIN_CHARGE' 
    , 1
    , 'false' 
    , estaff_subregion_desc
    , country_desc
    , issuer_institution_desc_grp
    , pmt_brand
    , pmt_type
    , bin   
)



-- select count(*) from chargeback_dim ;
-- ---235275
-- select count(*) from bill1;
-- ---15401986
-- select count(*) from bill2;
-- ---208005584



select * from

(
select 
pmt.txn_type
,pmt.processor
,pmt.billing_period_nbr
,pmt.token_used
,pmt.country_desc
,pmt.issuer_institution_desc_grp
,pmt.pmt_brand
,pmt.pmt_type
,pmt.bin
,pmt.approved_cnt
,coalesce(cb.chargeback_cnt,0) chargeback_cnt

from
    (
    select txn_type
    ,processor
    ,billing_period_nbr
    ,token_used
    ,country_desc
    ,issuer_institution_desc_grp
    ,pmt_brand
    ,pmt_type
    ,bin
    ,sum(approved_cnt) approved_cnt
    from bill1
    where approved_cnt>0
    group by 1,2,3,4,5,6,7,8,9
    )pmt

left join
    (
    select txn_type
    ,processor
    ,billing_period_nbr
    ,token_used
    ,country_desc
    ,issuer_institution_desc_grp
    ,pmt_brand
    ,pmt_type
    ,bin
    ,sum(chargeback_cnt) chargeback_cnt
    from chargeback_dim
    group by 1,2,3,4,5,6,7,8,9
    )cb
on
    cb.txn_type=pmt.txn_type
    and cb.processor=pmt.processor
---    and cb.retry_mode = pmt.retry_mode
    and cb.billing_period_nbr = pmt.billing_period_nbr
    and cb.token_used = pmt.token_used
    and cb.country_desc = pmt.country_desc
    and cb.issuer_institution_desc_grp = pmt.issuer_institution_desc_grp
    and cb.pmt_brand = pmt.pmt_brand
    and cb.pmt_type = pmt.pmt_type
    and cb.bin = pmt.bin
where pmt.issuer_institution_desc_grp in ('BANK OF AMERICA','WELLS FARGO')

    union all

select 
pmt.txn_type
,pmt.processor
,pmt.billing_period_nbr
,pmt.token_used
,pmt.country_desc
,pmt.issuer_institution_desc_grp
,pmt.pmt_brand
,pmt.pmt_type
,pmt.bin
,pmt.approved_cnt
,coalesce(cb.chargeback_cnt,0) chargeback_cnt

from
    (
    select txn_type
    ,processor
    ,2 billing_period_nbr
    ,token_used
    ,country_desc
    ,issuer_institution_desc_grp
    ,pmt_brand
    ,pmt_type
    ,bin
    ,sum(approved_cnt) approved_cnt
    from bill2
    where approved_cnt>0
    group by 1,2,3,4,5,6,7,8,9
    )pmt

left join
    (
    select txn_type
    ,processor
    ,2 billing_period_nbr
    ,token_used
    ,country_desc
    ,issuer_institution_desc_grp
    ,pmt_brand
    ,pmt_type
    ,bin
    ,sum(chargeback_cnt) chargeback_cnt
    from chargeback_dim
    group by 1,2,3,4,5,6,7,8,9
    )cb
on
    cb.txn_type=pmt.txn_type
    and cb.processor=pmt.processor
---    and cb.retry_mode = pmt.retry_mode
    and cb.billing_period_nbr = pmt.billing_period_nbr
    and cb.token_used = pmt.token_used
    and cb.country_desc = pmt.country_desc
    and cb.issuer_institution_desc_grp = pmt.issuer_institution_desc_grp
    and cb.pmt_brand = pmt.pmt_brand
    and cb.pmt_type = pmt.pmt_type
    and cb.bin = pmt.bin
where pmt.issuer_institution_desc_grp in ('BANK OF AMERICA','WELLS FARGO')
);

                                                           
------ Refund start from 2018-03-01
                                                           
with rr as(
select  nf_datetrunc('month', f.event_utc_date) as event_utc_month
        ,g.estaff_subregion_desc
        ,g.country_desc
        ,g.country_iso_code
        ,coalesce(ig.grouped_pmt_institution_desc, 'UNKNOWN') as issuer_desc_grouped
        ,coalesce(pmmd.dse_pmt_method, case when f.degenerate_values_map['mop_type'] = 'CC' then 'Card' else 'UNKNOWN' end) as pmt_method
        ,coalesce(pmmd.pmt_type_desc, 'UNKNOWN') as pmt_type
        ,coalesce(pmmd.affiliation, 'UNKNOWN') as pmt_brand
        ,coalesce(pmmd.clear_bin, 'UNKNOWN') as bin
        -- ,f.degenerate_values_map['chargeback_status'] as response_code_grouped  -- Can be 'NEW' or 'ERROR'
        -- ,case
        --       when pmmd.dse_pmt_method is null then 'UNKNOWN'
        --       when pmmd.dse_pmt_method <> 'Card' then 'Not Applicable'
        --       when pmmd.issuer_country_code is null then 'UNKNOWN'
        --       when pmmd.dse_pmt_method = '--' or pmmd.issuer_country_code = '--' then 'UNKNOWN'
        --       when pmmd.issuer_country_code = f.country_code then 'Local'
        --       else 'Foreign'
        --   end as foreign_local_card
        -- ,f.event_region_date as region_date
        ,count(distinct natural_key_map['req_proc_ref_id']) as refund_cnt

  from dse.pmt_events_hourly_f f
    left outer join dse.pmt_mop_metadata_d pmmd
      on f.dimension_sk_map['pmt_mop_metadata_sk'] = pmmd.pmt_mop_metadata_sk
    left outer join dse.pmt_institution_v2_d inst
      on pmmd.pmt_institution_sk = inst.pmt_institution_sk
    left outer join dse.pmt_institution_grouped_d ig
      on inst.pmt_institution_desc = ig.pmt_institution_desc
    left outer join dse.geo_country_d g
      on f.country_code = g.country_iso_code
  where 
    event_utc_date >=${event_start_date}
    and degenerate_values_map['txn_type']='REFUND'
    and degenerate_values_map['txn_status']='APPROVED'
    and pmt_event_type='payment_transaction'
  -- and   coalesce(pmmd.dse_pmt_method, case when f.degenerate_values_map['mop_type'] = 'CC' then 'Card' else 'UNKNOWN' end) = 'Card'
  -- and cast(degenerate_values_map['billing_period']as int)<=1
  group by 1,2,3,4,5,6,7,8,9
),


bill2 as
( select 
    nf_datetrunc('month', event_utc_date)  as  event_utc_month  
    , estaff_subregion_desc
    , country_desc
    , country_iso_code
    ,issuer_institution_desc_grp
    ,pmt_method
    ,pmt_type
    ,pmt_brand
    ,bin
    ,sum(approved_cnt)approved_cnt
    from dse.pmt_renewal_txn_agg
    where event_utc_date >= ${event_start_date}
    group by 1,2,3,4,5,6,7,8,9
)



select 
     b.event_utc_month  
    ,b.estaff_subregion_desc
    ,b.country_desc
    ,b.country_iso_code
    ,b.issuer_institution_desc_grp
    ,b.pmt_method
    ,b.pmt_type
    ,b.pmt_brand
    ,b.bin
    ,b.approved_cnt
    ,coalesce(rr.refund_cnt,0) refund_cnt

from bill2 b
left join rr
  on b.event_utc_month=rr.event_utc_month
  and b.estaff_subregion_desc = rr.estaff_subregion_desc
  and b.country_desc = rr.country_desc
  and b.country_iso_code = rr.country_iso_code
  and b.issuer_institution_desc_grp=rr.issuer_desc_grouped
  and b.pmt_method=rr.pmt_method
  and b.pmt_type=rr.pmt_type
  and b.pmt_brand=rr.pmt_brand
  and b.bin=rr.bin
  where b.issuer_institution_desc_grp in ('BANK OF AMERICA','WELLS FARGO');
