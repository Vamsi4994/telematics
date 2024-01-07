use stg_personal_secured;

DROP TABLE IF EXISTS tlm_automatic_tripd_r_stg;
CREATE  TABLE tlm_automatic_tripd_r_stg(
trip_id string,
vchr_nbr decimal(10,0),
point_timestamp bigint,	
point_local_dtm string,	
point_velocity decimal(6,3),	
rpm int	
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\;'
STORED AS INPUTFORMAT 
'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/apps/hive/warehouse/stg_personal_secured.db/tlm_automatic_tripd_r_stg';
  
use personal_secured;

DROP TABLE IF EXISTS tlm_automatic_tripd_r;
CREATE EXTERNAL TABLE tlm_automatic_tripd_r(
brand_cd string, 
program_nme string, 
product_cd string, 
trip_id string,
vchr_nbr decimal (10,0),
point_timestamp bigint, 
point_local_dtm string, 
point_velocity decimal(6,3), 
vehcl_rpm int 
)
PARTITIONED BY( 
 date_partition string)
CLUSTERED BY( 
 trip_id, 
 point_timestamp) 
SORTED BY( 
 trip_id ASC, 
 point_timestamp ASC) 
INTO 12 BUCKETS
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/personal/secured/telematics/automatic/tlm_automatic_tripd_r'
TBLPROPERTIES(
  'orc.compress'='SNAPPY');
  
use stg_personal_secured;

DROP TABLE IF EXISTS tlm_automatic_tripl_r_stg;
CREATE  TABLE tlm_automatic_tripl_r_stg(
trip_id string,
vchr_nbr decimal(10,0),
point_timestamp bigint,
point_local_dtm string, 
point_latitude decimal(20,16),
point_longitude decimal(20,16),
horzntl_accy decimal(20,16), 
tripl_speed decimal(20,16), 
point_hdng decimal(9,6),
point_source string 
)
ROW FORMAT DELIMITED 
 FIELDS TERMINATED BY '\;' 
STORED AS INPUTFORMAT 
 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
 '/apps/hive/warehouse/stg_personal_secured.db/tlm_automatic_tripl_r_stg';
 
use personal_secured;

DROP TABLE IF EXISTS tlm_automatic_tripl_r;
CREATE EXTERNAL TABLE tlm_automatic_tripl_r(
trip_id string,
vchr_nbr decimal(10,0),
point_timestamp bigint, 
point_local_dtm string, 
point_latitude decimal(20,16),
point_longitude decimal(20,16),
horzntl_accy decimal(20,16), 
tripl_speed decimal(20,16),
point_hdng decimal(9,6),
point_source string 
)
PARTITIONED BY ( 
  date_partition string)
CLUSTERED BY ( 
  trip_id, 
  point_timestamp) 
SORTED BY ( 
  trip_id ASC, 
  point_timestamp ASC) 
INTO 12 BUCKETS
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/personal/secured/telematics/automatic/tlm_automatic_tripl_r'
TBLPROPERTIES (
  'orc.compress'='SNAPPY');
  
use stg_personal_secured;

DROP TABLE IF EXISTS automatic_telematics_stg;
CREATE  TABLE automatic_telematics_stg(
vendor string, 
brand_cd string, 
program_nme string, 
product_cd string, 
type_id string, 
trip_id string, 
vchr_nbr decimal(10,0), 
point_dt string,
point_time string, 
point_latitude decimal(20,16), 
point_longitude decimal(20,16), 
point_gps_qlty string, 
point_gps_prcsn decimal(32,16), 
point_lcl_time_offst int, 
event_cd string, 
event_intnsty string, 
event_durtn int, 
odmtr_readng int, 
avg_speed_during_event int, 
max_acclrtn_during_event decimal(5,2), 
yds_trvld double, 
time_elpsd double, 
rnd_time_elpsd int, 
point_speed double, 
point_speed_vss decimal(6,3), 
point_speed_gps double, 
point_hdng decimal(32,16), 
avg_speed_from_prev_point smallint, 
road_typ string, 
point_gmt_dtm timestamp, 
point_gmt_int bigint, 
point_local_dtm timestamp, 
point_speed_diff double, 
point_source string, 
vehcl_rpm int, 
vndr_sys_trip_flg string, 
usr_trip_flg string, 
device_id string, 
trip_event_id string, 
trip_event_typ_cd string, 
src_date_partition string,
trip_start_date timestamp,
trip_end_date timestamp
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
 '/apps/hive/warehouse/stg_personal_secured.db/automatic_telematics_stg';

 
