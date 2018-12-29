set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.execution.engine=tez;
--set hive.vectorized.execution.enabled = true;
--set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.exec.dynamic.partition.mode=nonstrict;


DROP TABLE final_output;
CREATE EXTERNAL TABLE IF NOT EXISTS final_output (
   sub_cnt int,
   MKT_CD string,
   PRGM_NM    string,
   MPAA_RATE_CD string,
   STARS_RATE_CD string,
   TOTAL_ORIG_SEC_NBR double,
   PRGM_DESC string,
   PRGM_LANG_NM string,
   SRC_TYP_CD string,
   SHOW_TYP_CD    string,
   PRGM_RUN_TM_TXT double,
   EPSD_NBR string,
   PRGM_START_DT date,
   PRGM_START_TM timestamp,
   PRGM_END_DT date,
   PRGM_END_TM timestamp,
   TOTAL_PRGM_DURING_CNT bigint,
   PRGM_LIVE_DESC string,
   TV_RATE_CD string,
   PRMRE_FINLE_DESC string,
   NEW_PRGM_IND string,
   TV_PRGM_INSTNC_TYP string,
   REAL_NEW_PRGM_IND string,
   total_events int,
   spp_impressions double,
   daypart_hour int,
   PRGM_KEY bigint,
   SYSCODE_KEY int,
   SYSCODE string,
   SYSCODE_DMA_CD_KEY int,
   TV_PRGM_INSTNC_KEY int,
   PRGM_LCL_START_TS timestamp,
   PRGM_LCL_END_TS timestamp,
   STN_NORM_NM string,
   STN_ID string,
   STN_TYP_CD string,
   AD_SPRT_FLG string,
   STN_TM_SPND_OVRVIEW_CTGY_DESC string,
   STN_FEED_NM    string,
   STN_CULTURE_NM string,
   STN_QLTY_CD    string,
   STN_OWN_NM string,
   NTWRK_GENRE_CD string,
   CALCULATED_PRGM_DURING_CNT int
)
partitioned by (tuning_event_start_dt string)
stored as parquet
LOCATION 's3://srdata-lab/digital_emr_output/final_output/emr_station'
--LOCATION 's3://srdata-lab/emr_output/final_output/v3_station'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

msck repair table final_output;


/* Subset for specific markets */
drop table final_output_subset;
CREATE EXTERNAL TABLE IF NOT EXISTS final_output_subset (
   sub_cnt int,
   MKT_CD string,
   PRGM_NM string,
   MPAA_RATE_CD string,
   STARS_RATE_CD string,
   TOTAL_ORIG_SEC_NBR double,
   PRGM_DESC string,
   PRGM_LANG_NM string,
   SRC_TYP_CD string,
   SHOW_TYP_CD string,
   PRGM_RUN_TM_TXT double,
   EPSD_NBR string,
   PRGM_START_DT date,
   PRGM_START_TM timestamp,
   PRGM_END_DT date,
   PRGM_END_TM timestamp,
   TOTAL_PRGM_DURING_CNT bigint,
   PRGM_LIVE_DESC string,
   TV_RATE_CD string,
   PRMRE_FINLE_DESC string,
   NEW_PRGM_IND string,
   TV_PRGM_INSTNC_TYP string,
   REAL_NEW_PRGM_IND string,
   total_events int,
   spp_impressions double,
   daypart_hour int,
   PRGM_KEY bigint,
   SYSCODE_KEY int,
   SYSCODE string,
   SYSCODE_DMA_CD_KEY int,
   TV_PRGM_INSTNC_KEY int,
   PRGM_LCL_START_TS timestamp,
   PRGM_LCL_END_TS timestamp,
   STN_NORM_NM string,
   STN_ID string,
   STN_TYP_CD string,
   AD_SPRT_FLG string,
   STN_TM_SPND_OVRVIEW_CTGY_DESC string,
   STN_FEED_NM string,
   STN_CULTURE_NM string,
   STN_QLTY_CD string,
   STN_OWN_NM string,
   NTWRK_GENRE_CD string,
   CALCULATED_PRGM_DURING_CNT int
)
partitioned by (tuning_event_start_dt string)
stored as parquet
location 's3://srdata-lab/Matt/DeepARInputs/final_output_subset/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table final_output_subset partition (tuning_event_start_dt)
(select *
from final_output
--where MKT_CD in ('803','501','539','534','510','560','517','623','532','617'));
where MKT_CD in ('803','501'));

drop table am_station_to_network_rdm;
CREATE EXTERNAL TABLE am_station_to_network_rdm (
  stn_key int,
  network_key int,
  ntwrk_cd string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/am_station_to_network_rdm/';


drop table am_station_name_owner_dim;
CREATE EXTERNAL TABLE am_station_name_owner_dim(
STN_KEY int,
STN_ID string,
STN_TYP_CD string,
AD_SPRT_FLG string,
STN_AFLTN_TYP string,
STN_AFLTN_NM string,
STN_TM_SPND_OVRVIEW_CTGY_DESC string,
STN_FEED_NM string,
STN_CULTURE_NM string,
STN_QLTY_CD string,
STN_NORM_NM string,
STN_OWN_NM string,
EFF_START_DT date,
EFF_END_DT date,
CRE_TS timestamp,
UPDT_TS timestamp,
LD_SEQ_NBR int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
LOCATION
  's3://srdata-lab/am_station_name_owner_dim'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'transient_lastDdlTime'='1526156471');


drop table stnkey_stnid_ntwrkcd_map;
CREATE EXTERNAL TABLE stnkey_stnid_ntwrkcd_map (
  stn_key int,
  stn_id string,
  network_key int,
  ntwrk_cd string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/digital_DeepAR/stnkey_stnid_ntwrkcd_map';

insert overwrite table stnkey_stnid_ntwrkcd_map
(select a.*, b.stn_id
from am_station_to_network_rdm a left join am_station_name_owner_dim b
on a.stn_key = b.stn_key
);

-- Versioned? How many records in each?
--select count(distinct stn_key) from stnkey_stnid_ntwrkcd_map;
--select count(distinct stn_id) from stnkey_stnid_ntwrkcd_map;


drop table final_output_with_ntwrk_cd;
CREATE EXTERNAL TABLE IF NOT EXISTS final_output_with_ntwrk_cd (
   sub_cnt int,
   MKT_CD string,
   PRGM_NM string,
   MPAA_RATE_CD string,
   STARS_RATE_CD string,
   TOTAL_ORIG_SEC_NBR double,
   PRGM_DESC string,
   PRGM_LANG_NM string,
   SRC_TYP_CD string,
   SHOW_TYP_CD string,
   PRGM_RUN_TM_TXT double,
   EPSD_NBR string,
   PRGM_START_DT date,
   PRGM_START_TM timestamp,
   PRGM_END_DT date,
   PRGM_END_TM timestamp,
   TOTAL_PRGM_DURING_CNT bigint,
   PRGM_LIVE_DESC string,
   TV_RATE_CD string,
   PRMRE_FINLE_DESC string,
   NEW_PRGM_IND string,
   TV_PRGM_INSTNC_TYP string,
   REAL_NEW_PRGM_IND string,
   total_events int,
   spp_impressions double,
   daypart_hour int,
   PRGM_KEY bigint,
   SYSCODE_KEY int,
   SYSCODE string,
   SYSCODE_DMA_CD_KEY int,
   TV_PRGM_INSTNC_KEY int,
   PRGM_LCL_START_TS timestamp,
   PRGM_LCL_END_TS timestamp,
   STN_NORM_NM string,
   STN_ID string,
   STN_TYP_CD string,
   AD_SPRT_FLG string,
   STN_TM_SPND_OVRVIEW_CTGY_DESC string,
   STN_FEED_NM string,
   STN_CULTURE_NM string,
   STN_QLTY_CD string,
   STN_OWN_NM string,
   NTWRK_GENRE_CD string,
   CALCULATED_PRGM_DURING_CNT int,
   tuning_event_start_dt string,
   ntwrk_cd string
)
--partitioned by (tuning_evnt_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/final_output_with_ntwrk_cd/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table final_output_with_ntwrk_cd
--partition (tuning_evnt_start_dt)
(select a.*, b.ntwrk_cd
from final_output_subset a left join stnkey_stnid_ntwrkcd_map b
on a.STN_ID=b.STN_ID);

-- safety check
select tuning_event_start_dt, ntwrk_cd, NTWRK_GENRE_CD, STN_NORM_NM
from final_output_with_ntwrk_cd limit 10;


/* Case-when statement to recode NULL network genres */
drop table syscode_stn_ref_PRE;
create table syscode_stn_ref_PRE (
syscode string,
ntwrk_cd string,
STN_QLTY_CD string,
NTWRK_GENRE_RECD string,
NTWRK_GENRE_CD string,
STN_NORM_NM string,
MKT_CD string,
tuning_evnt_start_dt string
);

insert overwrite table syscode_stn_ref_PRE
(select syscode, ntwrk_cd, STN_QLTY_CD,
case when STN_NORM_NM in ('BET','MTV','VH1') then "Music"
 when substr(STN_NORM_NM,0,3) in ('TNT','TBS','MSG') then "Major Sports & Misc"
 when STN_NORM_NM in ('AMC','DISCOVERY','TLC','WE','OXYGEN','SYFI','HALLMARK','BRAVO') then "Movies & Series"
 else NTWRK_GENRE_CD end as NTWRK_GENRE_RECD,
NTWRK_GENRE_CD, STN_NORM_NM, MKT_CD, tuning_event_start_dt
from final_output_with_ntwrk_cd);

/* Ensure each Syscode and Stn combination has 1 to 1 relationships with Cat fields */
drop table syscode_stn_ref;
CREATE TABLE syscode_stn_ref (
syscode string,
ntwrk_cd string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
MinDate date,
STN_NORM_NM string,
MKT_CD string
);

insert overwrite table syscode_stn_ref
(select syscode, ntwrk_cd, min(STN_QLTY_CD), min(NTWRK_GENRE_CD), min(NTWRK_GENRE_RECD),  min(to_date(from_unixtime(unix_timestamp(tuning_evnt_start_dt, "yyyy-MM-dd")))) as MinDate,
min(STN_NORM_NM) as STN_NORM_NM, max(MKT_CD) as MKT_CD
from syscode_stn_ref_PRE
group by syscode, ntwrk_cd);

-- Check here that this data makes sense
select ntwrk_cd, NTWRK_GENRE_RECD, STN_NORM_NM
from syscode_stn_ref limit 10;

/* Create DateRef from Final_Output */
drop table DateRef_pre;
CREATE EXTERNAL TABLE IF NOT EXISTS DateRef_pre (
tuning_evnt_start_dt date,
daypart_hour int,
Count_Recs int
)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARDateRef/DateRef_pre'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table DateRef_pre
(select tuning_event_start_dt, daypart_hour, count(*) as Count_Recs
from final_output_with_ntwrk_cd
group by tuning_event_start_dt, daypart_hour
);

drop table DateRef;
CREATE EXTERNAL TABLE IF NOT EXISTS DateRef (
tuning_event_start_dt date,
daypart_hour int
)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARDateRef/DateRef_Dynamic'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table DateRef
(select tuning_evnt_start_dt, daypart_hour
from DateRef_pre
);



drop table syscode_ntwrk_dateref;
CREATE TABLE syscode_ntwrk_dateref (
daypart_hour int,
syscode string,
ntwrk_cd string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
MinDate date,
STN_NORM_NM string,
MKT_CD string
)
partitioned by (tuning_event_start_dt date);

insert overwrite table syscode_ntwrk_dateref partition (tuning_event_start_dt)
(select daypart_hour, syscode, ntwrk_cd, STN_QLTY_CD, NTWRK_GENRE_CD, NTWRK_GENRE_RECD, MinDate, STN_NORM_NM, MKT_CD, tuning_event_start_dt
from dateref, syscode_stn_ref where tuning_event_start_dt >= MinDate);

-- START HERE TO FIX

set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

drop table dateconvert;
CREATE TABLE dateconvert (
sub_cnt int,
syscode string,
ntwrk_cd string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
daypart_hour int,
total_events double,
spp_impressions double
)
partitioned by (tuning_event_start_dt date)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/dateconvert/Network'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table dateconvert partition (tuning_event_start_dt)
(select sub_cnt, syscode, ntwrk_cd, STN_QLTY_CD, NTWRK_GENRE_CD, daypart_hour, total_events, spp_impressions,
to_date(from_unixtime(unix_timestamp(tuning_event_start_dt, "yyyy-MM-dd"))) as tuning_event_start_dt
from final_output_with_ntwrk_cd);

/* Take sub_cnt from latest month available */
drop table latest_sub_cnt;
CREATE TABLE latest_sub_cnt (
syscode string,
sub_cnt int
);

insert overwrite table latest_sub_cnt
(select syscode, min(sub_cnt) as sub_cnt
--from dateconvert where month(tuning_evnt_start_dt) in (select month(max(tuning_evnt_start_dt)) from final_output_1mkt)
from dateconvert
where month(tuning_event_start_dt) = 9
and year(tuning_event_start_dt) in
    (select year(max(tuning_event_start_dt))
     from final_output_with_ntwrk_cd)
group by syscode);

--select year(max(tuning_evnt_start_dt)), month(max(tuning_evnt_start_dt)) from final_output_1mkt;



/* Create MinDate summary table */
drop table syscode_date;
CREATE TABLE syscode_date (
syscode string,
ntwrk_cd string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
daypart_hour int,
total_events double,
spp_impressions double
)
partitioned by (tuning_event_start_dt date)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/syscode_date/Network'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table syscode_date partition (tuning_event_start_dt)
(select syscode, ntwrk_cd, STN_QLTY_CD, NTWRK_GENRE_CD, daypart_hour, sum(total_events) as total_events,
sum(spp_impressions) as spp_impressions, tuning_event_start_dt
from dateconvert
GROUP BY tuning_event_start_dt, syscode, ntwrk_cd, STN_QLTY_CD, NTWRK_GENRE_CD, daypart_hour);


/* Outer join to match syscode min date data to DateRef table */
drop table deepar_input_pre;
CREATE TABLE deepar_input_pre (
daypart_hour int,
syscode string,
ntwrk_cd string,
syscode_ntwrk string,
MinDate timestamp,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
STN_NORM_NM string,
MKT_CD string,
total_events double,
spp_impressions double
)
partitioned by (tuning_event_start_dt timestamp)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/DeepAR_Input_Pre/Network'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table deepar_input_pre partition (tuning_event_start_dt)
(SELECT a.daypart_hour, a.syscode, a.ntwrk_cd, CONCAT(a.syscode, " - ", a.ntwrk_cd) as syscode_ntwrk, a.MinDate, a.STN_QLTY_CD, a.NTWRK_GENRE_CD, a.NTWRK_GENRE_RECD, a.STN_NORM_NM, a.MKT_CD,
COALESCE(b.total_events,0), COALESCE(b.spp_impressions,0), a.tuning_event_start_dt
FROM syscode_ntwrk_dateref a LEFT OUTER JOIN syscode_date b
on a.tuning_event_start_dt = b.tuning_event_start_dt and a.daypart_hour = b.daypart_hour
and a.syscode = b.syscode
and a.ntwrk_cd = b.ntwrk_cd);

--select count(distinct syscode_ntwrk) from deepar_input_pre;

drop table am_market_dim;
CREATE EXTERNAL TABLE am_market_dim(
MKT_KEY int,
MKT_CD string,
MKT_DESC string,
CABLE_TRAK_NM string,
NIELSEN_NM string,
ECLIPSE_NM string,
LD_SEQ_NBR int,
CRE_TS timestamp,
UPDT_TS timestamp,
ETL_PRCS_DT timestamp)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = '|'
)
LOCATION
  's3://srdata-lab/am_market_dim'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'transient_lastDdlTime'='1526156471');



drop table deepar_input_ntwrk;
CREATE EXTERNAL TABLE IF NOT EXISTS deepar_input_ntwrk (
daypart_hour int,
syscode string,
ntwrk_cd string,
syscode_ntwrk string,
MinDate timestamp,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
STN_NORM_NM string,
MKT_CD string,
NIELSEN_NM string,
total_events double,
spp_impressions double,
sub_cnt int
)
partitioned by (tuning_event_start_dt timestamp)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/DeepAR_Input/DeepAR_Input_Network/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table deepar_input_ntwrk partition (tuning_event_start_dt)
(SELECT a.daypart_hour, a.syscode, a.ntwrk_cd, a.syscode_ntwrk, a.MinDate, a.STN_QLTY_CD, a.NTWRK_GENRE_CD, a.NTWRK_GENRE_RECD, a.STN_NORM_NM, a.MKT_CD, c.NIELSEN_NM,
a.total_events, a.spp_impressions, COALESCE(b.sub_cnt,0), a.tuning_event_start_dt
FROM deepar_input_pre a LEFT JOIN latest_sub_cnt b
on a.syscode = b.syscode
left join am_market_dim c
on a.MKT_CD = c.MKT_CD);
