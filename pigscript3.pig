register /home/gopalkrishna/INSTALL/pig-0.15.0/lib/piggybank.jar
define xpath org.apache.pig.piggybank.evaluation.xml.XPath();
XMLData = load '/userstory/input/retaildata.xml' using org.apache.pig.piggybank.storage.XMLLoader('Sheet1') as (x:chararray);
D = foreach XMLData generate (float)(xpath(x, 'Sheet1/RTLID')) as RTLID,(xpath(x, 'Sheet1/RTLNAME')) as RTLNAME,(xpath(x, 'Sheet1/TYPEOFCRAWLING')) as TYPEOFCRAWLING,(xpath(x, 'Sheet1/PRD_SALED_LOC')) as PRD_SALED_LOC;
C = FOREACH D generate $0,$1,$2,$3;


HDMData = load '/REATAIL_OUTPUT/HighDemandMarket-r-00004' using PigStorage(',') as (RTLID:float,RTLNAME:chararray,
TYPEOFCRAWLING:chararray,PROD_URL:chararray,TITLE:chararray,SALEPRICE:double,REGPRICE:double,REBATEPERCENT:double,STOCKINFO:chararray);
HDMData1 = FOREACH HDMData generate $0,$1,$2,$3,$4,$5,$6,$7,$8;
sortData = order HDMData1 by $0 desc parallel 5;
--Limitdata = limit sortData 20;
STORE sortData INTO '/HDMData_OUTPUT123';


OMSData = load '/REATAIL_OUTPUT/OngingMarketStrategy-r-00001' using PigStorage(',') as (RTLID:float,RTLNAME:chararray,
TYPEOFCRAWLING:chararray,PROD_URL:chararray,TITLE:chararray,SALEPRICE:double,REGPRICE:double,REBATEPERCENT:double,STOCKINFO:chararray);
OMSData1 = FOREACH OMSData generate $0,$1,$2,$3,$4,$5,$6,$7,$8;
OsortData = order OMSData1 by $0 desc parallel 1;
STORE OsortData INTO '/OMSData_OUTPUT123';


RPData = load '/REATAIL_OUTPUT/OtherProducts-r-00004' using PigStorage(',') as (RTLID:float,RTLNAME:chararray,
TYPEOFCRAWLING:chararray,PROD_URL:chararray,TITLE:chararray,SALEPRICE:double,REGPRICE:double,REBATEPERCENT:double,STOCKINFO:chararray);
RPData1 = FOREACH RPData generate $0,$1,$2,$3,$4,$5,$6,$7,$8;
RsortData = order RPData1 by $0 desc parallel 5;
STORE RsortData INTO '/RPData_OUTPUT123';


WP = load '/REATAIL_OUTPUT/ReliableProducts-r-00002' using PigStorage(',') as (RTLID:float,RTLNAME:chararray,
TYPEOFCRAWLING:chararray,PROD_URL:chararray,TITLE:chararray,SALEPRICE:double,REGPRICE:double,REBATEPERCENT:double,STOCKINFO:chararray);
WPData1 = FOREACH WP generate $0,$1,$2,$3,$4,$5,$6,$7,$8;
WsortData = order WPData1 by $0 desc parallel 5;
STORE WsortData INTO '/WPData_OUTPUT123';


OP = load '/REATAIL_OUTPUT/WealthyProducts-r-00004' using PigStorage(',') as (RTLID:float,RTLNAME:chararray,
TYPEOFCRAWLING:chararray,PROD_URL:chararray,TITLE:chararray,SALEPRICE:double,REGPRICE:double,REBATEPERCENT:double,STOCKINFO:chararray);
OPData1 = FOREACH OP generate $0,$1,$2,$3,$4,$5,$6,$7,$8;
OPsortData = order OPData1 by $0 desc parallel 5;
STORE OPsortData INTO '/OPData_OUTPUT123';


--clubData = union HDMData,OMSData,RPData,WP,OP;
--store clubData into '/userstory/pigoutputclubdata';
--illustrate clubData;
--describe D;


--OrdData = order D by RTLID;
--OrdData2 = order clubData by RTLID;
--store clubData into 'cluboutput';

JoinData = join HDMData1 by RTLID,C by (int)$0;
--STORE JoinData INTO '/JOIN_OUTPUT123';
distdata = distinct JoinData;
describe distdata;
--DistData = distinct JoinData;

--describe DistData;
--SortData = order DistData by $0 desc;
--store  JoinData into '/userstory/pigoutput12';
--SortData = order JoinData by RTLID desc;
--store DistData into '/userstory/pigoutputdistdata';
--store SortData into '/userstory/pigoutputsortdata';
--ordata = order D by RTLNAME;
--store D into 'retailoutput20';

