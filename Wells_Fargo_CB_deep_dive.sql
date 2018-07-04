---- start from 1/1/2018
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
