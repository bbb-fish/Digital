set hive.exec.dynamic.partition.mode=nonstrict;
--SET dfs.block.size=134217728;
--SET parquet.block.size=134217728;
--SET hive.exec.reducers.bytes.per.reducer=134217728;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.exec.reducers.bytes.per.reducer=128000000;

drop table my_iptv_ptef;
CREATE EXTERNAL TABLE IF NOT EXISTS my_iptv_ptef (
  IPTV_TUNING_EVENT_KEY bigint,
  TV_PRGM_INSTNC_KEY int,
  SBSC_GUID_KEY int,
  SESN_ID string,
  STN_KEY smallint,
  DMA_CD_KEY smallint,
  ZIP_KEY smallint,
  PRGM_KEY int,
  DAYPART_KEY smallint,
  PRGM_LCL_START_DT date,
  PRGM_LCL_START_TM string,
  PRGM_LCL_START_TS timestamp,
  PRGM_LCL_START_SEC_OF_DAY_NBR int,
  PRGM_LCL_END_DT date,
  PRGM_LCL_END_TM string,
  PRGM_LCL_END_TS timestamp,
  PRGM_LCL_END_SEC_OF_DAY_NBR int,
  PRGM_LCL_DUR_CNT int,
  TUNING_EVENT_TYP_CD int,
  TUNING_EVENT_START_DT string,
  TUNING_EVENT_START_TM string,
  TUNING_EVENT_START_TS timestamp,
  TUNING_EVENT_START_SEC_OF_DAY_NBR int,
  TUNING_EVENT_END_DT string,
  TUNING_EVENT_END_TM string,
  TUNING_EVENT_END_TS timestamp,
  TUNING_EVENT_END_SEC_OF_DAY_NBR int,
  TUNING_EVENT_DUR_CNT int,
  TUNE_IN_CNT int,
  TUNE_OUT_CNT int,
  CLNT_TYP_KEY int,
  CNTRY_CD_KEY int,
  LOCAL_TM_ZN string,
  ORIG_TUNING_EVENT_START_DT date,
  ORIG_TUNING_EVENT_START_TS timestamp,
  ORIG_TUNING_EVENT_END_TS timestamp,
  PROCESS_DT date,
  ONEAPP_FLG string
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT  "com.hadoop.mapred.DeprecatedLzoTextInputFormat"
OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
LOCATION '/tmp/iptv';

--drop not necessary since the cluster likes to consistently fail
--drop table output_parquet;
CREATE EXTERNAL TABLE IF NOT EXISTS output_parquet (
  IPTV_TUNING_EVENT_KEY bigint,
  TV_PRGM_INSTNC_KEY int,
  SBSC_GUID_KEY int,
  SESN_ID string,
  STN_KEY smallint,
  DMA_CD_KEY smallint,
  ZIP_KEY smallint,
  PRGM_KEY int,
  DAYPART_KEY smallint,
  PRGM_LCL_START_DT date,
  PRGM_LCL_START_TM string,
  PRGM_LCL_START_TS timestamp,
  PRGM_LCL_START_SEC_OF_DAY_NBR int,
  PRGM_LCL_END_DT date,
  PRGM_LCL_END_TM string,
  PRGM_LCL_END_TS timestamp,
  PRGM_LCL_END_SEC_OF_DAY_NBR int,
  PRGM_LCL_DUR_CNT int,
  TUNING_EVENT_TYP_CD int,
  --TUNING_EVENT_START_DT string,
  TUNING_EVENT_START_TM string,
  TUNING_EVENT_START_TS timestamp,
  TUNING_EVENT_START_SEC_OF_DAY_NBR int,
  TUNING_EVENT_END_DT string,
  TUNING_EVENT_END_TM string,
  TUNING_EVENT_END_TS timestamp,
  TUNING_EVENT_END_SEC_OF_DAY_NBR int,
  TUNING_EVENT_DUR_CNT int,
  TUNE_IN_CNT int,
  TUNE_OUT_CNT int,
  CLNT_TYP_KEY int,
  CNTRY_CD_KEY int,
  LOCAL_TM_ZN string,
  ORIG_TUNING_EVENT_START_DT date,
  ORIG_TUNING_EVENT_START_TS timestamp,
  ORIG_TUNING_EVENT_END_TS timestamp,
  PROCESS_DT date,
  ONEAPP_FLG string
  ) PARTITIONED BY (TUNING_EVENT_START_DT string)
STORED AS PARQUET
LOCATION 's3://srdata-lab/Praveen/iptv/parquet'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert into output_parquet partition(TUNING_EVENT_START_DT) (
select IPTV_TUNING_EVENT_KEY,
  TV_PRGM_INSTNC_KEY,
  SBSC_GUID_KEY,
  SESN_ID,
  STN_KEY,
  DMA_CD_KEY,
  ZIP_KEY,
  PRGM_KEY,
  DAYPART_KEY,
  PRGM_LCL_START_DT,
  PRGM_LCL_START_TM,
  PRGM_LCL_START_TS,
  PRGM_LCL_START_SEC_OF_DAY_NBR,
  PRGM_LCL_END_DT,
  PRGM_LCL_END_TM,
  PRGM_LCL_END_TS,
  PRGM_LCL_END_SEC_OF_DAY_NBR,
  PRGM_LCL_DUR_CNT,
  TUNING_EVENT_TYP_CD,
  --TUNING_EVENT_START_DT,
  TUNING_EVENT_START_TM,
  TUNING_EVENT_START_TS,
  TUNING_EVENT_START_SEC_OF_DAY_NBR,
  TUNING_EVENT_END_DT,
  TUNING_EVENT_END_TM,
  TUNING_EVENT_END_TS,
  TUNING_EVENT_END_SEC_OF_DAY_NBR,
  TUNING_EVENT_DUR_CNT,
  TUNE_IN_CNT,
  TUNE_OUT_CNT,
  CLNT_TYP_KEY,
  CNTRY_CD_KEY,
  LOCAL_TM_ZN,
  ORIG_TUNING_EVENT_START_DT,
  ORIG_TUNING_EVENT_START_TS,
  ORIG_TUNING_EVENT_END_TS,
  PROCESS_DT,
  ONEAPP_FLG,
replace(TUNING_EVENT_START_DT,'/','-')
from my_iptv_ptef
);
