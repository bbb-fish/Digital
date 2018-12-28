set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

DROP TABLE final_output;
CREATE EXTERNAL TABLE IF NOT EXISTS final_output (
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
  NTWRK_CD string,
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
partitioned by (tuning_evnt_start_dt string)
stored as parquet
LOCATION 's3://srdata-lab/digital_emr_output/final_output/emr_station'
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
partitioned by (tuning_evnt_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARInputs/final_output_subset/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table final_output_subset partition (tuning_evnt_start_dt)
(select *
from final_output
where MKT_CD ! in ('503','513','521','522','524','531','533','540','543','551','553','556','557','563',
'575','576','583','602','604','609','610','611','613','619','622','632','639','640','642','659','669','673',
'676','698','702','705','716','717','724','737','740','751','752','754','755','756','758','759','762','764',
'766','767','773','790','801','802','807','810','811','813','819','820','828','855','862','866','868','881'));

/* Case-when statement to recode NULL network genres */
drop table syscode_stn_ref_PRE;
create table syscode_stn_ref_PRE (
syscode string,
STN_ID string,
STN_QLTY_CD string,
NTWRK_GENRE_RECD string,
NTWRK_GENRE_CD string,
STN_NORM_NM string,
MKT_CD string,
tuning_evnt_start_dt string
)
stored as parquet;

insert overwrite table syscode_stn_ref_PRE
(select syscode, STN_ID, STN_QLTY_CD,
case when STN_NORM_NM in ('BET','MTV','VH1') then "Music"
 when substr(STN_NORM_NM,0,3) in ('TNT','TBS','MSG') then "Major Sports & Misc"
 when STN_NORM_NM in ('AMC','DISCOVERY','TLC','WE','OXYGEN','SYFI','HALLMARK','BRAVO') then "Movies & Series"
 else NTWRK_GENRE_CD end as NTWRK_GENRE_RECD,
NTWRK_GENRE_CD, STN_NORM_NM, MKT_CD, tuning_evnt_start_dt
from final_output_subset);

/* Ensure each Syscode and Stn combination has 1 to 1 relationships with Cat fields */
drop table syscode_stn_ref;
CREATE TABLE syscode_stn_ref (
syscode string,
STN_ID string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
MinDate date,
STN_NORM_NM string,
MKT_CD string
)
stored as parquet;

insert overwrite table syscode_stn_ref
(select syscode, STN_ID, min(STN_QLTY_CD), min(NTWRK_GENRE_CD), min(NTWRK_GENRE_RECD),  min(to_date(from_unixtime(unix_timestamp(tuning_evnt_start_dt, "yyyy-MM-dd")))) as MinDate,
min(STN_NORM_NM) as STN_NORM_NM, max(MKT_CD) as MKT_CD
from syscode_stn_ref_PRE
group by syscode, STN_ID);

/* Create DateRef from Final_Output */
drop table DateRef_pre;
CREATE EXTERNAL TABLE IF NOT EXISTS DateRef_pre (
tuning_evnt_start_dt date,
daypart_hour int,
Count_Recs int
)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARDateRef/DateRef_pre/'
--location 's3://srdata-lab/Matt/DeepARDateRef/DateRef_pre'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table DateRef_pre
(select tuning_evnt_start_dt, daypart_hour, count(*) as Count_Recs
from final_output
group by tuning_evnt_start_dt, daypart_hour
);

drop table DateRef;
CREATE EXTERNAL TABLE IF NOT EXISTS DateRef (
tuning_evnt_start_dt date,
daypart_hour int
)
stored as parquet
location 's3://srdata-lab/digital_DeepAR/DeepARDateRef/DateRef_Dynamic/'
--location 's3://srdata-lab/Matt/DeepARDateRef/DateRef_dynamic'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table DateRef
(select tuning_evnt_start_dt, daypart_hour
from DateRef_pre
);

drop table syscode_stn_dateref;
CREATE TABLE syscode_stn_dateref (
daypart_hour int,
syscode string,
STN_ID string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
MinDate date,
STN_NORM_NM string,
MKT_CD string
)
stored as parquet
partitioned by (tuning_evnt_start_dt date);

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table syscode_stn_dateref partition (tuning_evnt_start_dt)
(select daypart_hour, syscode, STN_ID, STN_QLTY_CD, NTWRK_GENRE_CD, NTWRK_GENRE_RECD, MinDate, STN_NORM_NM, MKT_CD, tuning_evnt_start_dt
from dateref, syscode_stn_ref where tuning_evnt_start_dt >= MinDate);

drop table dateconvert;
CREATE TABLE dateconvert (
sub_cnt int,
syscode string,
STN_ID string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
daypart_hour int,
total_events double,
spp_impressions double
)
stored as parquet
partitioned by (tuning_evnt_start_dt date);

insert overwrite table dateconvert partition (tuning_evnt_start_dt)
(select sub_cnt, syscode, STN_ID, STN_QLTY_CD, NTWRK_GENRE_CD, daypart_hour, total_events, spp_impressions,
to_date(from_unixtime(unix_timestamp(tuning_evnt_start_dt, "yyyy-MM-dd"))) as tuning_evnt_start_dt
from final_output_subset);

/* Take sub_cnt from latest month available */
drop table latest_sub_cnt;
CREATE TABLE latest_sub_cnt (
syscode string,
sub_cnt int
)
stored as parquet;

insert overwrite table latest_sub_cnt
(select syscode, min(sub_cnt) as sub_cnt
from dateconvert where month(tuning_evnt_start_dt) in (select month(max(tuning_evnt_start_dt)) from final_output_subset)
group by syscode);

/* Create MinDate summary table */
drop table syscode_date;
CREATE TABLE syscode_date (
syscode string,
STN_ID string,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
daypart_hour int,
total_events double,
spp_impressions double
)
stored as parquet
partitioned by (tuning_evnt_start_dt date);

insert overwrite table syscode_date partition (tuning_evnt_start_dt)
(select syscode, STN_ID, STN_QLTY_CD, NTWRK_GENRE_CD, daypart_hour, sum(total_events) as total_events,
sum(spp_impressions) as spp_impressions, tuning_evnt_start_dt
from dateconvert
GROUP BY tuning_evnt_start

/* Outer join to match syscode min date data to DateRef table */
drop table deepar_input_pre;
CREATE TABLE deepar_input_pre (
daypart_hour int,
syscode string,
STN_ID string,
syscode_stn string,
MinDate timestamp,
STN_QLTY_CD string,
NTWRK_GENRE_CD string,
NTWRK_GENRE_RECD string,
STN_NORM_NM string,
MKT_CD string,
total_events double,
spp_impressions double
)
stored as parquet
partitioned by (tuning_evnt_start_dt timestamp);

insert overwrite table deepar_input_pre partition (tuning_evnt_start_dt)
(SELECT a.daypart_hour, a.syscode, a.STN_ID, CONCAT(a.syscode, " - ", a.stn_id) as syscode_stn, a.MinDate, a.STN_QLTY_CD, a.NTWRK_GENRE_CD, a.NTWRK_GENRE_RECD, a.STN_NORM_NM, a.MKT_CD,
COALESCE(b.total_events,0), COALESCE(b.spp_impressions,0), a.tuning_evnt_start_dt
FROM syscode_stn_dateref a LEFT OUTER JOIN syscode_date b
on a.tuning_evnt_start_dt = b.tuning_evnt_start_dt and a.daypart_hour = b.daypart_hour
and a.syscode = b.syscode
and a.STN_ID = b.STN_ID);

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

  /* Join in Sub_Cnt separetly as to not delete daypart rows */
  /* Added NIELSEN_NM on Oct 2 2018 */
  drop table deepar_input_raleigh;
  CREATE EXTERNAL TABLE IF NOT EXISTS deepar_input_raleigh (
  daypart_hour int,
  syscode string,
  STN_ID string,
  syscode_stn string,
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
  partitioned by (tuning_evnt_start_dt timestamp)
  stored as parquet
  location 's3://srdata-lab/digital_DeepAR/DeepARInputs/DeepAR_Input/DeepAR_All_Mkt'
  TBLPROPERTIES ("parquet.compression"="SNAPPY");

  insert overwrite table deepar_input_raleigh partition (tuning_evnt_start_dt)
  (SELECT a.daypart_hour, a.syscode, a.STN_ID, a.syscode_stn, a.MinDate, a.STN_QLTY_CD, a.NTWRK_GENRE_CD, a.NTWRK_GENRE_RECD, a.STN_NORM_NM, a.MKT_CD, c.NIELSEN_NM,
  a.total_events, a.spp_impressions, COALESCE(b.sub_cnt,0), a.tuning_evnt_start_dt
  FROM deepar_input_pre a LEFT JOIN latest_sub_cnt b
  on a.syscode = b.syscode
  left join am_market_dim c
  on a.MKT_CD = c.MKT_CD);
