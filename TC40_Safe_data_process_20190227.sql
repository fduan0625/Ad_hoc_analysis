-- tail -n +3 TC40_0111.txt > TC40_1_redu.txt
-- cut -d'|' -f4,12,14,16 TC40_1_redu.txt > TC40_0111_reduce_column.txt

-- tail -n +3 TC40_0112.txt > TC40_2_redu.txt
-- cut -d'|' -f4,12,14,16 TC40_2_redu.txt > TC40_0112_reduce_column.txt

-- tail -n +3 TC40_0113.txt > TC40_3_redu.txt
-- cut -d'|' -f4,12,14,16 TC40_3_redu.txt > TC40_0113_reduce_column.txt

-- tail -n +3 TC40_0114.txt > TC40_4_redu.txt
-- cut -d'|' -f4,12,14,16 TC40_4_redu.txt > TC40_0114_reduce_column.txt


-- cut -d'|' -f4,12,15,46 safe.txt > safe_redu.txt

CREATE EXTERNAL TABLE fduan.TC40_safe_file(
referenceID string,
experiation_date int,
sale_amt double,
fraud_reason string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3n://netflix-dataoven-prod-users/hive/warehouse/fduan.db/TC40_safe_file'


create table  fduan.TC40_safe_file_new as
(select base.*
,case when fraud_reason in ('6', 'randomenotestokeep') then 'TC40_Fraudulent_Use'
      when fraud_reason = '1' then 'TC40_Card_Stolen'
      when fraud_reason = '2' then 'TC40_Card_Not_Received'
      when fraud_reason = '3' then 'TC40_Fraudulent_Application'
      when fraud_reason = '4' then 'TC40_Issuer_Reported_Counterfeit'
      when fraud_reason = '5' then 'TC40_ATO'
      when fraud_reason = '7' then 'TC40_Issuers_Clearinghouse_Service'
      when fraud_reason = '9' then 'TC40_Acquirer_reported_counterfeit'
      else fraud_reason end as fraud_reason_text
from fduan.TC40_safe_file base
)

        -- hive_path = 'metacat://prodhive/fduan/fraud_bin_refit_score_pred'
        -- kg.transport.Transporter().source(df).target(hive_path).execute()
        -- print ("Uploading dataframe with " + str(output_df.shape[0]) + " rows to " + hive_path)


create table fduan.tc40_safe_account_list as
  (
select other_properties['customerId'] as acct_id
,other_properties['isoCountryCode'] as country_code
,other_properties['referenceId'] as referenceId
,a.fraud_reason_text
from default.payment_txn_event base
inner join fduan.TC40_safe_file_new a
  on base.other_properties['referenceId'] = a.referenceID
where base.dateint>=20190101
)



create table fduan.tc40_account_fraud_cb_status as
  (select base.*
    ,a.fraud_flag
    ,(case when cb.account_id is not null then 1 else 0 end) as cb_t_f

    from fduan.tc40_safe_account_list base
    left join dse.subscrn_d a
      on cast(base.acct_id as bigint) = cast(a.account_id as bigint)
      and a.signup_date>=20181101
    
    left join etl.pmt_chargeback_txn_sum cb
      on cast(base.acct_id as bigint) = cast(cb.account_id as bigint)
      and cb_event_utc_date >= 20190101
where base.acct_id <> 'null'
and a.account_id is not null
and cb.account_id is not null
)



select fraud_flag, cb_t_f,fraud_reason_text
,count(distinct acct_id) as num_accts
from fduan.tc40_account_fraud_cb_status
group by 1,2,3;
  
