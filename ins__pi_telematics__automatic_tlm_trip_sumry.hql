set hive.execution.engine=tez;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.intermediate.compression.type=BLOCK;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.optimize.skewjoin=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.use.nonstaged=false;
set hive.vectorized.execution.reduce.groupby.enabled=true;
set mapred.reduce.tasks=40;
set mapred.max.split.size=67108864;
set hive.skewjoin.key=100000;
set hive.skewjoin.mapjoin.map.tasks=10000;
set hive.skewjoin.mapjoin.min.split=33554432;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set mapred.map.child.java.opts="-Xmx5G";
set mapred.reduce.child.java.opts="-Xmx5G";

use pi_telematics_summary;
insert into table pi_telematics_summary.tlm_trip_sumry partition (point_dt) select 
 a.vendor,
 a.brand_cd,
 a.program_nme,
 a.product_cd, 
 a.trip_id,
 a.vchr_nbr,
 count(distinct (case when trip_event_typ_cd = '03' then trip_event_id end)),
 count(distinct (case when trip_event_typ_cd = '02' then trip_event_id end)),
 count(distinct (case when trip_event_typ_cd = '01' then trip_event_id end)),
 count(distinct (case when trip_event_typ_cd = '04' then trip_event_id end)),
 count(distinct (case when trip_event_typ_cd = '05' then trip_event_id end)),
 count(distinct (case when trip_event_typ_cd = '06' then trip_event_id end)),
 sum(a.nighttime)/1760 as night_mileage_tot,
 sum(a.calc_mileage)/1760 as calc_mileage_tot,
 sum(cast(a.yds_trvld as decimal(6,3))) as yds_trvld_tot,
 min(a.point_local_dtm) as trip_st_time,
 max(a.point_local_dtm) as trip_end_time,
 cast((max(a.point_gmt_int) - min(a.point_gmt_int))/60000 as decimal(10, 6)) as trip_durtn,
 min(substring(a.point_local_dtm,0,10)) as local_dt,
 a.trip_cmpltd_ind,
 a.vndr_sys_trip_flg,
 a.usr_trip_flg,
 min(a.src_date_partition),
 min(a.point_dt)
 from ( select 
 vendor,
 brand_cd,
 program_nme,
 product_cd,
 trip_id,
 vchr_nbr,
 yds_trvld,
 point_local_dtm,
 point_gmt_int,
 vndr_sys_trip_flg,
 usr_trip_flg,
 trip_event_typ_cd,
 trip_event_id,
 yds_trvld as calc_mileage,
 case when  substring(point_local_dtm,12,8) between '00:00:00' and '03:59:59'  then yds_trvld else '0' end as nighttime,
 'y' as trip_cmpltd_ind,
 src_date_partition,
 point_dt 
 from stg_personal_secured.automatic_telematics_stg)a group by a.vendor, a.brand_cd, a.program_nme, a.product_cd,a.trip_id, a.vchr_nbr, a.vndr_sys_trip_flg,
 a.usr_trip_flg,a.trip_cmpltd_ind; 
