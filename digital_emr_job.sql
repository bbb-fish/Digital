set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.tez.auto.reducer.parallelism = true;


drop table iptv_ptef_in;
CREATE EXTERNAL TABLE IF NOT EXISTS iptv_ptef_in (
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
  --  TUNING_EVENT_START_DT string,
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
partitioned by (tuning_event_start_dt string)
STORED AS PARQUET
LOCATION 's3://srdata-lab/Praveen/iptv/parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

--msck repair loads in all the table partitions
msck repair table iptv_ptef_in;

drop table am_zip_lowest_lvl_sys_cd;
CREATE EXTERNAL TABLE IF NOT EXISTS am_zip_lowest_lvl_sys_cd (
    ZIP_KEY int,
    SYSCODE_KEY int,
    RETL_UNIT_KEY int,
    SYSCODE_DMA_CD_KEY int,
    RETAIL_EFF_START_DT Date,
    RETAIL_EFF_END_DT Date,
    SYS_EFF_START_DT Date,
    SYS_EFF_END_DT Date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AM_ZIP_LOWEST_LVL_SYS_CD';

drop table am_syscode_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS am_syscode_dim (
     SYSCODE_KEY int,
     SYSCODE string,
     SYSCODE_DESC string,
     SYSCODE_TYP_KEY int,
     SYSCODE_UNIV_EST double,
     EFF_START_DT date,
     EFF_END_DT date,
     LD_SEQ_NBR int,
     CRE_TS timestamp,
     UPDT_TS timestamp,
     ETL_PRCS_DT date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AM_SYSCODE_DIM';

drop table am_program_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS am_program_dim (
    PRGM_KEY bigint,
    PRGM_ID	    string,
    PRGM_NM	    string,
    MPAA_RATE_CD string,
    STARS_RATE_CD string,
    TOTAL_ORIG_SEC_NBR double,
    PRGM_DESC string,
    PRGM_LANG_NM string,
    SRC_TYP_CD string,
    SHOW_TYP_CD	    string,
    ORIG_AIR_DT bigint,
    PRGM_RUN_TM_TXT double,
    EPSD_NBR string,
    PROCESS_DT bigint
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AM_PROGRAM_DIM';

drop table am_tv_program_instance_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS am_tv_program_instance_dim (
    TV_PRGM_INSTNC_KEY bigint,
    PRGM_KEY bigint,
    STN_KEY bigint,
    PRGM_START_DT bigint,
    PRGM_START_TM bigint,
    PRGM_END_DT bigint,
    PRGM_END_TM bigint,
    PRGM_LCL_START_TM bigint,
    PRGM_LCL_END_DT bigint,
    PRGM_LCL_END_TM bigint,
    TOTAL_PRGM_DURING_CNT bigint,
    PRGM_LIVE_DESC string,
    TV_RATE_CD string,
    PRMRE_FINLE_DESC string,
    NEW_PRGM_IND string,
    TV_PRGM_INSTNC_TYP string,
    REAL_NEW_PRGM_IND string,
    IN_SESN_IND string,
    SRS_SESN_NM string,
    SRS_SESN_START_DT bigint,
    SRS_SESN_END_DT bigint,
    PROCESS_DT bigint
    )
partitioned by (prgm_lcl_start_dt date)
STORED AS PARQUET
LOCATION 's3://srdata-lab/am_tv_program_instance_dim_day/parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
--loading in table partitions

msck repair table am_tv_program_instance_dim;

drop table am_station_name_owner_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS am_station_name_owner_dim (
    STN_KEY int,
    STN_ID string,
    STN_TYP_CD string,
    AD_SPRT_FLG string,
    STN_AFLTN_TYP string,
    STN_AFLTN_NM string,
    STN_TM_SPND_OVRVIEW_CTGY_DESC string,
    STN_FEED_NM	    string,
    STN_CULTURE_NM string,
    STN_QLTY_CD	    string,
    STN_NORM_NM	    string,
    STN_OWN_NM string,
    EFF_START_DT Date,
    EFF_END_DT Date,
    CRE_TS timestamp,
    UPDT_TS timestamp,
    LD_SEQ_NBR int
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AM_STATION_NAME_OWNER_DIM/';

--join to AM Market dim
drop table am_market_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS am_market_dim (
    MKT_KEY	    int,
    MKT_CD string,
    MKT_DESC string,
    CABLE_TRAK_NM string,
    NIELSEN_NM string,
    ECLIPSE_NM string,
    LD_SEQ_NBR int,
    CRE_TS timestamp,
    UPDT_TS timestamp,
    ETL_PRCS_DT	    Date
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AM_DMA_DIM/';

drop table ad_insertable_station_status;
CREATE EXTERNAL TABLE IF NOT EXISTS ad_insertable_station_status (
    NTWRK_CD string,
    SYSCODE_KEY int,
    RETL_UNIT_KEY int,
    STN_KEY int,
    AD_INSERTABLE_FLG string,
    HH_INSERTABLE_FLG string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = "|",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/ipredict_prod/tables/dims/AD_INSERTABLE_STATION_STATUS/';

drop table network_genre;
CREATE EXTERNAL TABLE IF NOT EXISTS network_genre (
    NTWRK_CD string,
    NTWRK_NM string,
    NTWRK_GENRE_POS_NBR int,
    NTWRK_GENRE_CD string,
    CRE_TS timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = '\"',
  "escapeChar" = "\\"
) STORED AS TEXTFILE
LOCATION 's3://srdata-lab/network_genre/';

--initial summarization to drastically decrease the number of rows we do the next few joins on
drop table initial_summarize;
CREATE EXTERNAL TABLE IF NOT EXISTS initial_summarize (
    total_events int,
    spp_impressions double,
    daypart_hour int,
    STN_KEY int,
    PRGM_KEY bigint,
    DMA_CD_KEY int,
    TV_PRGM_INSTNC_KEY int,
    PRGM_LCL_START_TS timestamp,
    PRGM_LCL_END_TS timestamp,
    ZIP_KEY int
)
partitioned by (tuning_event_start_dt string)
stored as parquet
LOCATION 's3://srdata-lab/emr_output/initial_summarize/emr_station'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
insert overwrite table initial_summarize partition(tuning_event_start_dt)
(
    select count(IPTV_TUNING_EVENT_KEY) as total_events, sum(TUNING_EVENT_DUR_CNT/3600.0) as spp_impressions, hour(TUNING_EVENT_START_TS) as daypart_hour, STN_KEY, PRGM_KEY, DMA_CD_KEY,
    TV_PRGM_INSTNC_KEY, PRGM_LCL_START_TS, PRGM_LCL_END_TS, ZIP_KEY, tuning_event_start_dt
    from iptv_ptef_in
    --where tuning_event_start_dt between '${hiveconf:START_DATE}' and '${hiveconf:END_DATE}'
    where tuning_event_start_dt between '2018-02-01' and '2018-02-02'
    group by tuning_event_start_dt, hour(TUNING_EVENT_START_TS), STN_KEY, PRGM_KEY, DMA_CD_KEY, TV_PRGM_INSTNC_KEY, PRGM_LCL_START_TS, PRGM_LCL_END_TS, ZIP_KEY
);

-- Bobby run the rest from here
set hive.exec.reducers.bytes.per.reducer=16000000;
--now that we've done an initial summarization, we can join in
--the tables needed before we summarize one more time
drop table joined_syscode_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS joined_syscode_dim (
    SYSCODE string,
    SYSCODE_DESC string,
    SYSCODE_TYP_KEY int,
    SYSCODE_UNIV_EST double,
    EFF_START_DT date,
    EFF_END_DT date,
    LD_SEQ_NBR int,
    CRE_TS timestamp,
    UPDT_TS timestamp,
    ETL_PRCS_DT date,
    NTWRK_CD string,
    AD_INSERTABLE_FLG string,
    total_events int,
    spp_impressions double,
    daypart_hour int,
    STN_KEY int,
    PRGM_KEY bigint,
    DMA_CD_KEY int,
    TV_PRGM_INSTNC_KEY int,
    PRGM_LCL_START_TS timestamp,
    PRGM_LCL_END_TS timestamp,
    ZIP_KEY int,
    SYSCODE_KEY int,
    RETL_UNIT_KEY int,
    SYSCODE_DMA_CD_KEY int,
    RETAIL_EFF_START_DT Date,
    RETAIL_EFF_END_DT Date,
    SYS_EFF_START_DT Date,
    SYS_EFF_END_DT Date,
    NTWRK_GENRE_CD string
)
partitioned by (tuning_event_start_dt string)
stored as parquet
LOCATION 's3://srdata-lab/digital_emr_output/joined_syscode_dim/emr_station/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
--inner join to am_zip_lowest_lvl_sys_cd to exclude any syscodes
--that aren't in the lowest_lvl_sys_cd dim
--inner join to ad_insertable_station_status for the same reason
insert overwrite table joined_syscode_dim partition(tuning_event_start_dt) (
    select SYSCODE, SYSCODE_DESC, SYSCODE_TYP_KEY, SYSCODE_UNIV_EST, EFF_START_DT,
    EFF_END_DT, LD_SEQ_NBR, s.CRE_TS, UPDT_TS, ETL_PRCS_DT, iss.NTWRK_CD, AD_INSERTABLE_FLG,
    total_events, spp_impressions, daypart_hour, iptv.STN_KEY, PRGM_KEY, DMA_CD_KEY, TV_PRGM_INSTNC_KEY,
    PRGM_LCL_START_TS, PRGM_LCL_END_TS, iptv.ZIP_KEY, z.SYSCODE_KEY, z.RETL_UNIT_KEY,
    SYSCODE_DMA_CD_KEY, RETAIL_EFF_START_DT,
    RETAIL_EFF_END_DT, SYS_EFF_START_DT, SYS_EFF_END_DT, NTWRK_GENRE_CD, tuning_event_start_dt
    from initial_summarize iptv
    inner join (select distinct * from am_zip_lowest_lvl_sys_cd where from_unixtime(unix_timestamp(RETAIL_EFF_END_DT, 'yyyy/MM/dd'), 'yyyy-MM-dd') = '2099-12-31'
    and from_unixtime(unix_timestamp(SYS_EFF_END_DT, 'yyyy/MM/dd'), 'yyyy-MM-dd') = '2099-12-31') z
    on (iptv.ZIP_KEY = z.ZIP_KEY)
    inner join (select distinct * from ad_insertable_station_status) iss
    on (z.RETL_UNIT_KEY = iss.RETL_UNIT_KEY
    and iptv.STN_KEY = iss.STN_KEY
    and z.SYSCODE_KEY = iss.SYSCODE_KEY)
    left join am_syscode_dim s on z.SYSCODE_KEY = s.SYSCODE_KEY
    left join (select distinct ntwrk_cd, NTWRK_GENRE_CD from network_genre where NTWRK_GENRE_POS_NBR = 0) ng on iss.ntwrk_cd = ng.ntwrk_cd
);

--up to here now
--join to station dim then summarize again
drop table joined_station_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS joined_station_dim (
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
    SYSCODE string,
    SYSCODE_DESC string,
    SYSCODE_TYP_KEY int,
    SYSCODE_UNIV_EST double,
    EFF_START_DT Date,
    EFF_END_DT Date,
    LD_SEQ_NBR int,
    CRE_TS timestamp,
    UPDT_TS timestamp,
    ETL_PRCS_DT date,
    NTWRK_CD string,
    AD_INSERTABLE_FLG string,
    total_events int,
    spp_impressions double,
    daypart_hour int,
    STN_KEY int,
    PRGM_KEY bigint,
    DMA_CD_KEY int,
    TV_PRGM_INSTNC_KEY int,
    PRGM_LCL_START_TS timestamp,
    PRGM_LCL_END_TS timestamp,
    ZIP_KEY int,
    SYSCODE_KEY int,
    RETL_UNIT_KEY int,
    SYSCODE_DMA_CD_KEY int,
    RETAIL_EFF_START_DT Date,
    RETAIL_EFF_END_DT Date,
    SYS_EFF_START_DT Date,
    SYS_EFF_END_DT Date,
    NTWRK_GENRE_CD string
)
partitioned by (tuning_event_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_emr_output/joined_station_dim/emr_station/'
TBLPROPERTIES("parquet.compression"="SNAPPY");
insert overwrite table joined_station_dim partition(tuning_event_start_dt)
(
    select STN_ID, STN_TYP_CD, AD_SPRT_FLG, STN_AFLTN_TYP, STN_AFLTN_NM, STN_TM_SPND_OVRVIEW_CTGY_DESC,
    STN_FEED_NM, STN_CULTURE_NM, STN_QLTY_CD, STN_NORM_NM, STN_OWN_NM, aptef.*
    from joined_syscode_dim aptef
    left join am_station_name_owner_dim asnod on aptef.STN_KEY = asnod.STN_KEY and
    aptef.tuning_event_start_dt between asnod.eff_start_dt and asnod.eff_end_dt
    --where AD_INSERTABLE_FLG = 'Y'
);

--after joining in tables needed to summarize to the correct granularity, we can summarize again
drop table summarized_fact;
create external table if not exists summarized_fact (
    total_events int,
    spp_impressions double,
    daypart_hour int,
    PRGM_KEY bigint,
    SYSCODE_KEY int,
    SYSCODE string,
    DMA_CD_KEY int,
    TV_PRGM_INSTNC_KEY int,
    PRGM_LCL_START_TS timestamp,
    PRGM_LCL_END_TS timestamp,
    STN_NORM_NM string,
    NTWRK_CD string,
    STN_ID string,
    STN_TYP_CD string,
    AD_SPRT_FLG string,
    STN_AFLTN_TYP string,
    STN_AFLTN_NM string,
    STN_TM_SPND_OVRVIEW_CTGY_DESC string,
    STN_FEED_NM string,
    STN_CULTURE_NM string,
    STN_QLTY_CD string,
    STN_OWN_NM string,
    NTWRK_GENRE_CD string
)
partitioned by (tuning_event_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_emr_output/final_summary/emr_station/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

--impression counts are summed
insert overwrite table summarized_fact partition(tuning_event_start_dt)
(
    select sum(total_events) as total_events, sum(spp_impressions) as spp_impressions, daypart_hour, PRGM_KEY, SYSCODE_KEY, SYSCODE, SYSCODE_DMA_CD_KEY,
    TV_PRGM_INSTNC_KEY, PRGM_LCL_START_TS, PRGM_LCL_END_TS, STN_NORM_NM, NTWRK_CD, STN_ID, STN_TYP_CD, AD_SPRT_FLG,
    STN_AFLTN_TYP, STN_AFLTN_NM, stn_tm_spnd_ovrview_ctgy_desc, STN_FEED_NM,
    STN_CULTURE_NM, STN_QLTY_CD, STN_OWN_NM, NTWRK_GENRE_CD, tuning_event_start_dt
    from joined_station_dim
    group by tuning_event_start_dt, SYSCODE_DMA_CD_KEY, daypart_hour, PRGM_KEY, SYSCODE_KEY, SYSCODE, TV_PRGM_INSTNC_KEY, PRGM_LCL_START_TS, PRGM_LCL_END_TS, STN_NORM_NM, NTWRK_CD,
    STN_ID, STN_AFLTN_TYP, STN_AFLTN_NM, stn_tm_spnd_ovrview_ctgy_desc, STN_FEED_NM,
    STN_CULTURE_NM, STN_QLTY_CD, STN_OWN_NM, STN_TYP_CD, AD_SPRT_FLG, NTWRK_GENRE_CD
);

--Final joins after final summary
drop table joined_program_station_dims;
CREATE EXTERNAL TABLE IF NOT EXISTS joined_program_station_dims (
    PRGM_ID string,
    PRGM_NM string,
    MPAA_RATE_CD string,
    STARS_RATE_CD string,
    TOTAL_ORIG_SEC_NBR double,
    PRGM_DESC string,
    PRGM_LANG_NM string,
    SRC_TYP_CD string,
    SHOW_TYP_CD string,
    ORIG_AIR_DT bigint,
    PRGM_RUN_TM_TXT double,
    EPSD_NBR string,
    program_dim_PROCESS_DT bigint,
    PRGM_START_DT bigint,
    PRGM_START_TM bigint,
    PRGM_END_DT bigint,
    PRGM_END_TM bigint,
    PRGM_LCL_START_TM bigint,
    PRGM_LCL_END_DT bigint,
    PRGM_LCL_END_TM bigint,
    TOTAL_PRGM_DURING_CNT bigint,
    PRGM_LIVE_DESC string,
    TV_RATE_CD string,
    PRMRE_FINLE_DESC string,
    NEW_PRGM_IND string,
    TV_PRGM_INSTNC_TYP string,
    REAL_NEW_PRGM_IND string,
    IN_SESN_IND string,
    SRS_SESN_NM string,
    SRS_SESN_START_DT bigint,
    SRS_SESN_END_DT bigint,
    tv_program_PROCESS_DT bigint,
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
    STN_AFLTN_TYP string,
    STN_AFLTN_NM string,
    STN_TM_SPND_OVRVIEW_CTGY_DESC string,
    STN_FEED_NM string,
    STN_CULTURE_NM string,
    STN_QLTY_CD string,
    STN_OWN_NM string,
    NTWRK_GENRE_CD string
)
partitioned by (tuning_event_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_emr_output/joined_program_station_dims/emr_station/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table joined_program_station_dims partition (tuning_event_start_dt)
(
    select PRGM_ID, PRGM_NM, MPAA_RATE_CD, STARS_RATE_CD, TOTAL_ORIG_SEC_NBR, PRGM_DESC, PRGM_LANG_NM, SRC_TYP_CD, SHOW_TYP_CD,
    ORIG_AIR_DT, PRGM_RUN_TM_TXT, EPSD_NBR, apd.PROCESS_DT as program_dim_PROCESS_DT, PRGM_START_DT, PRGM_START_TM, PRGM_END_DT,
    PRGM_END_TM, PRGM_LCL_START_TM, PRGM_LCL_END_DT, PRGM_LCL_END_TM, TOTAL_PRGM_DURING_CNT, PRGM_LIVE_DESC, TV_RATE_CD,
    PRMRE_FINLE_DESC, NEW_PRGM_IND, TV_PRGM_INSTNC_TYP, REAL_NEW_PRGM_IND, IN_SESN_IND, SRS_SESN_NM, SRS_SESN_START_DT, SRS_SESN_END_DT, atpid.PROCESS_DT as tv_program_PROCESS_DT,
    aptef.*
    from summarized_fact aptef
    left outer join am_tv_program_instance_dim atpid on
    aptef.tuning_event_start_dt = atpid.prgm_lcl_start_dt and aptef.PRGM_KEY = atpid.PRGM_KEY and aptef.TV_PRGM_INSTNC_KEY = atpid.TV_PRGM_INSTNC_KEY
    left outer join am_program_dim apd on aptef.PRGM_KEY = apd.PRGM_KEY
);

drop table subscribers_by_syscode;
create external table if not exists subscribers_by_syscode (
    clndr_dt date,
    nielsen_nm string,
    legacy_co_nm string,
    syscode_key int,
    sub_cnt int
)
stored as parquet
location 's3://srdata-lab/emr_output/subscribers_by_syscode/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

--join to market dim and subscribers dim
drop table joined_market_program_dim;
CREATE EXTERNAL TABLE IF NOT EXISTS joined_market_program_dim (
    sub_cnt int,
    MKT_CD string,
    MKT_DESC string,
    CABLE_TRAK_NM string,
    NIELSEN_NM string,
    ECLIPSE_NM string,
    market_LD_SEQ_NBR int,
    market_CRE_TS timestamp,
    market_UPDT_TS timestamp,
    ETL_PRCS_DT Date,
    PRGM_ID string,
    PRGM_NM string,
    MPAA_RATE_CD string,
    STARS_RATE_CD string,
    TOTAL_ORIG_SEC_NBR double,
    PRGM_DESC string,
    PRGM_LANG_NM string,
    SRC_TYP_CD string,
    SHOW_TYP_CD string,
    ORIG_AIR_DT bigint,
    PRGM_RUN_TM_TXT double,
    EPSD_NBR string,
    program_dim_PROCESS_DT bigint,
    PRGM_START_DT bigint,
    PRGM_START_TM bigint,
    PRGM_END_DT bigint,
    PRGM_END_TM bigint,
    PRGM_LCL_START_TM bigint,
    PRGM_LCL_END_DT bigint,
    PRGM_LCL_END_TM bigint,
    TOTAL_PRGM_DURING_CNT bigint,
    PRGM_LIVE_DESC string,
    TV_RATE_CD string,
    PRMRE_FINLE_DESC string,
    NEW_PRGM_IND string,
    TV_PRGM_INSTNC_TYP string,
    REAL_NEW_PRGM_IND string,
    IN_SESN_IND string,
    SRS_SESN_NM string,
    SRS_SESN_START_DT bigint,
    SRS_SESN_END_DT bigint,
    tv_program_PROCESS_DT bigint,
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
    STN_AFLTN_TYP string,
    STN_AFLTN_NM string,
    STN_TM_SPND_OVRVIEW_CTGY_DESC string,
    STN_FEED_NM string,
    STN_CULTURE_NM string,
    STN_QLTY_CD string,
    STN_OWN_NM string,
    NTWRK_GENRE_CD string
)
partitioned by (tuning_event_start_dt string)
stored as parquet
location 's3://srdata-lab/digital_emr_output/joined_market_program_dims/emr_station/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

insert overwrite table joined_market_program_dim partition(tuning_event_start_dt)
(
    select
    sub_cnt,
    MKT_CD, MKT_DESC, CABLE_TRAK_NM, NIELSEN_NM, ECLIPSE_NM, amd.LD_SEQ_NBR as market_LD_SEQ_NBR,
    amd.CRE_TS as market_CRE_TS, amd.UPDT_TS as market_UPDT_TS, ETL_PRCS_DT, jpsd.*
    from joined_program_station_dims jpsd
    left outer join am_market_dim amd on jpsd.SYSCODE_DMA_CD_KEY = amd.MKT_KEY
    left join (
        select clndr_dt, b.syscode, sum(sub_cnt) as sub_cnt
        from subscribers_by_syscode a join am_syscode_dim b
        on a.syscode_key = b.syscode_key
        group by clndr_dt, a.syscode_key, b.syscode
    ) subs
    on (month(jpsd.tuning_event_start_dt) = month(clndr_dt) and year(jpsd.tuning_event_start_dt) = year(clndr_dt) and jpsd.syscode = subs.syscode)
);

drop table final_output;
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
partitioned by (tuning_event_start_dt string)
stored as parquet
LOCATION 's3://srdata-lab/digital_emr_output/final_output/emr_station/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
--for final_output we cast some dates from integers to actual dates
--(this is because hive originally could not bring them in as dates)


insert overwrite table final_output partition (tuning_event_start_dt) (
    select
    sub_cnt,
    MKT_CD,
    PRGM_NM,
    MPAA_RATE_CD,
    STARS_RATE_CD,
    TOTAL_ORIG_SEC_NBR,
    PRGM_DESC,
    PRGM_LANG_NM,
    SRC_TYP_CD,
    SHOW_TYP_CD,
    PRGM_RUN_TM_TXT,
    EPSD_NBR,
    to_date(from_unixtime(prgm_start_dt)) as prgm_start_dt,
    from_unixtime(prgm_start_tm) as prgm_start_tm,
    to_date(from_unixtime(prgm_end_dt)) as prgm_end_dt,
    from_unixtime(prgm_end_tm) as prgm_end_tm,
    TOTAL_PRGM_DURING_CNT,
    PRGM_LIVE_DESC,
    TV_RATE_CD,
    PRMRE_FINLE_DESC,
    NEW_PRGM_IND,
    TV_PRGM_INSTNC_TYP,
    REAL_NEW_PRGM_IND,
    total_events,
    spp_impressions,
    daypart_hour,
    PRGM_KEY,
    SYSCODE_KEY,
    SYSCODE,
    SYSCODE_DMA_CD_KEY,
    TV_PRGM_INSTNC_KEY,
    PRGM_LCL_START_TS,
    PRGM_LCL_END_TS,
    STN_NORM_NM,
    NTWRK_CD,
    STN_ID,
    STN_TYP_CD,
    AD_SPRT_FLG,
    STN_TM_SPND_OVRVIEW_CTGY_DESC,
    STN_FEED_NM,
    STN_CULTURE_NM,
    STN_QLTY_CD,
    STN_OWN_NM,
    NTWRK_GENRE_CD,
    --case when STN_NORM_NM in ('BET','MTV','VHI') then "Music"                                                       --Recoding some of the genres to improve model accuracy
    --when STN_NORM_NM in ('TNT','TBS','MSG') then "Major Sports & Misc"
    --when STN_NORM_NM in ('AMC','DISCOVERY','TLC','WE','OXYGEN','SYFI','HALLMARK','BRAVO') then "Movies & Series"
    --else NTWRK_GENRE_CD end as NTWRK_GENRE_RECD,
    (unix_timestamp(PRGM_LCL_END_TS) - unix_timestamp(PRGM_LCL_START_TS)) / 60 as CALCULATED_PRGM_DURING_CNT, --PRGM_DURING_CNT has many null values, so created a calculated one so we always have a value
    tuning_event_start_dt
    from joined_market_program_dim
);
