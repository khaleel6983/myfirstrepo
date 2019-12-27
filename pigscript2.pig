register /home/gopalkrishna/INSTALL/pig-0.15.0/lib/piggybank.jar;
define xpath org.apache.pig.piggybank.evaluation.xml.XPath();
XMLData = load '/home/gopalkrishna/userstory/retaildata.xml' using org.apache.pig.piggybank.storage.XMLLoader('Sheet1') as 
(x:chararray);
D = foreach XMLData generate (xpath(x, 'Sheet1/RTLID')) as RTLID,(xpath(x, 'Sheet1/RTLNAME')) as RTLNAME,
(xpath(x, 'Sheet1/TYPEOFCRAWLING')) as TYPEOFCRAWLING,(xpath(x, 'Sheet1/PRD_SALED_LOC')) as PRD_SALED_LOC;
store D into 'retailoutput23';
