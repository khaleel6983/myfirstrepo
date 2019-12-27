set hive.cli.print.current.db=true;

DROP DATABASE retailproject CASCADE;
create database IF NOT EXISTS retailproject;
use retailproject;

!echo ******************************************************************************************************;
!echo ========================= THIS IS BASE TABLE FOR PIG-OUTPUT======================================;
!echo *******************************************************************************************************;
create external table if not exists retaildata_nonpart(RTLID string,RTLNAME string,TYPEOFCRAWLING string,PROD_URL string,
TITLE string,SALEPRICE string,REGPRICE string,REBATEPERCENT string,STOCKINFO string,
RTLID2 string,RTLNAME2 string,TYPEOFCRAWLING2 string,PROD_SALED_LOC string)
row format delimited
fields terminated by'\t'
lines terminated by '\n'
stored as textfile location '/pigoutput/';
--load data inpath '/userstory2/pigoutput/part-r-00000' into table retaildata_nonpart;


!echo ******************************************************************************************************;
!echo ============================CREATING DYNAMIC PARTITION FOR BASE TABLE=================================;
!echo ******************************************************************************************************;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;
create external table if not exists retaildata2(RTLID string,RTLNAME string,PROD_URL string,
TITLE string,SALEPRICE string,REGPRICE string,REBATEPERCENT string,STOCKINFO string,
RTLID2 string,RTLNAME2 string,TYPEOFCRAWLING2 string,PROD_SALED_LOC string) 
partitioned by (TYPEOFCRAWLING string)
clustered by (RTLID) into 2 buckets
row format delimited
fields terminated by'\t'
lines terminated by '\n'
stored as textfile location '/hivetables2';

!echo ******************************************************************************************************;
!echo ==================INSERTING THE FILE FROM BASE TABLE TO DYNAMIC PARTITION=============================;
!echo ******************************************************************************************************;


insert overwrite table retaildata2 partition(TYPEOFCRAWLING) select RTLID,RTLNAME,PROD_URL,TITLE,
SALEPRICE,REGPRICE,REBATEPERCENT,STOCKINFO,RTLID2,RTLNAME2,TYPEOFCRAWLING2,PROD_SALED_LOC,TYPEOFCRAWLING 
from retaildata_nonpart;

!echo ******************************************************************************************************;
!echo =========================DYNAMIC PARTITION PART IS COMPLETED WITH TYPEOFCRAWLING===============================;
!echo ******************************************************************************************************;

!echo ******************************************************************************************************;
!echo =========================HIVE TABLE OUTPUT===============================;
!echo ******************************************************************************************************;


select * from retaildata2 limit 10;
