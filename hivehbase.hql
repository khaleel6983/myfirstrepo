!echo ************************************************************************;
!echo                 		CREATING DATABASE IN HBASE                    ;
!echo ************************************************************************;
use retailproject;
set hive.cli.print.current.db=true;

!echo "************************************************************************;
!echo "                	Add the jars for HBASE-HIVE Integration                ;
!echo "************************************************************************;
add jar /home/gopalkrishna/INSTALL/apache-hive-1.2.1-bin/lib/guava-14.0.1.jar;
add jar /home/gopalkrishna/INSTALL/apache-hive-1.2.1-bin/lib/zookeeper-3.4.6.jar;
add jar /home/gopalkrishna/INSTALL/apache-hive-1.2.1-bin/lib/hive-hbase-handler-1.2.1.jar;

!echo "************************************************************************;
!echo "       creating HIVE EXTERNAL TABLE with HBASE COLUMN MAPPING           ;
!echo "************************************************************************;


drop table if exists retailtabforhbase;

create external table retailtabforhbase (RTLID string,RTLNAME string,TYPEOFCRAWLING string,PROD_URL string,
TITLE string,SALEPRICE string,REGPRICE string,REBATEPERCENT string,STOCKINFO string,
RTLID2 string,RTLNAME2 string,TYPEOFCRAWLING2 string,PROD_SALED_LOC string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping"=":key,retaildetails:RTLNAME,retaildetails:TYPEOFCRAWLING,retaildetails:PROD_URL,
retaildetails:TITLE,retaildetails:SALEPRICE,retaildetails:REGPRICE,retaildetails:REBATEPERCENT,retaildetails:STOCKINFO,
retaildetails:RTLID2,retaildetails:RTLNAME2,retaildetails:TYPEOFCRAWLING2,retaildetails:PROD_SALED_LOC")
TBLPROPERTIES("hbase.table.name"="retailtable");
describe formatted retailtabforhbase;

!echo ************************************************************************;
!echo        		OVERWRITING DATA TO CHILD TABLE       		      ;
!echo ************************************************************************;


insert overwrite table retailtabforhbase select * from retaildata2;
select * from retailtabforhbase limit 10;

!echo "************************************************************************;
!echo "      		 VALIDATING THE DATA IN CHILD TABLE                    	;
!echo "************************************************************************;
select count(*) from retailtabforhbase;

