# SCD1-Implementation-in-HDFS
SCD1 implementation in HDFS for an Online Orders table taking the change in http info for the same customer id


<h2>------Source Data-------</h2>

  Source data is provided as a flat file on the daily basis which the followign columns
  (custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code )
  
<h3>--Data Cleaning--</h3>
    
    Data cleaning is done through python to make the data ready for an sql sourcee table load.
    
  
  
  <h3>---Sql Flat File Load---</h3>
    <h4>Sql source table creation:</h4>
    create table daily1 ( cust_id int,username varchar(50),quote_count int,ip varchar(50),entry_time date,prp_1 int,prp_2 int,prp_3 int,ms int,http_type                      
    varchar(50),purchase_category varchar(50),total_count int,purchase_sub_category varchar(50),http_info varchar(100),status_code int,file_name varchar(30));
    
   <h4>Properties to be set for loading directly from flat file to sql table:</h4>
    SET GLOBAL local_infile=1;
    exit;
    mysql --local-infile=1 -uroot -pWelcome@123

   <h4>These Flat file tables are loaded into sql using the :</h4>
   
    load data local infile '/home/saif/LFS/Project_1/Datasets_Project_1/cleaned_datasets/1.csv' into table daily_data1  fields terminated by ',' lines terminated by '\n'     
    (cust_id, username,@quote_count ,ip,@entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,file_name)  set 
    quote_count=replace(trim(@quote_count),' ',''), entry_time=str_to_date(date_format(str_to_date(@entry_time,'%d/%b/%Y:%H'),"%d-%m-%Y"),"%d-%m-%Y");
    
    create table daily_bkp ( cust_id int,username varchar(50),quote_count int,ip varchar(50),entry_time date,prp_1 int,prp_2 int,prp_3 int,ms int,http_type varchar(50),purchase_category varchar(50),total_count int,purchase_sub_category varchar(50),http_info varchar(100),status_code int,file_name varchar(30));
    
insert into daily_bkp select * from daily_data1;

create table daily_txt ( cust_id varchar(50),username varchar(50),quote_count varchar(20),ip varchar(50),entry_time varchar(30),prp_1 varchar(20),prp_2 varchar(50),prp_3 varchar(50),ms int,http_type varchar(50),purchase_category varchar(50),total_count varchar(20),purchase_sub_category varchar(100),http_info varchar(100),status_code varchar(50),file_name varchar(50),year varchar(50),month varchar(50),day varchar(50));

<h3>-----HIVE-------</h3>

<h4> -- Starting Hive Services -- </h4>

nohup hive --service metastore & hive

<h4> -- Using Database -- </h4>

use cohort_c9;

<h4> -- Creating a Table -- </h4>

create table daily ( cust_id int,username string,quote_count int,ip string,entry_time date,prp_1 int,prp_2 int,prp_3 int,ms int,http_type string,purchase_category string,total_count int,purchase_sub_category string,http_info string,status_code int,file_name string) row format delimited fields terminated by ',';


<h3>----sqoop Load----</h3>

sqoop import  --connect jdbc:mysql://localhost:3306/testing?useSSL=False --username root --password Welcome@123  --query "select * from daily" --hive-import --hive-table cohort_c9.daily --delete-target-dir --target-dir /user/saif/HFS/Project_1/ -m 1


<h3>----hive----</h3>
<h4>----setting dynaic properties---</h4>

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;  

<h4>----creating ext table----</h4>
CREATE EXTERNAL table daily_ext ( cust_id int,username string,quote_count int,ip string,entry_time date,prp_1 int,prp_2 int,prp_3 int,ms int,http_type string,purchase_category string,total_count int,purchase_sub_category string,http_info string,status_code int,file_name string) partitioned by (year string,month string,day string) row format delimited fields terminated by ',' stored as orc ;

<h4>---setting dynaic pratition size---</h4>
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

<h4>----Inserting into the table----</h4>

insert into daily_ext partition(year,month,day) select cust_id,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,file_name,year(entry_time),month(entry_time),day(entry_time) from daily_data1;  

<h4>--acid properties---</h4>
set hive.support.concurrency=true;
set hive.enforece.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.auto.convert.join=false;
set hive.merge.cardinality.check=false;

<h4>--change external table to managed table----</h4>
ALTER TABLE daily_ext set TBLPROPERTIES('EXTERNAL'='FALSE','transactional'='true');

<h4>----incremental update----</h4>
MERGE INTO daily_ext pe USING daily p ON pe.cust_id=p.cust_id WHEN MATCHED THEN UPDATE SET http_info=p.http_info WHEN NOT MATCHED THEN INSERT VALUES (p.cust_id,p.username,p.quote_count,p.ip,p.entry_time,p.prp_1,p.prp_2,p.prp_3,p.ms,p.http_type,p.purchase_category,p.total_count,p.purchase_sub_category,p.http_info,p.status_code,p.file_name,year(p.entry_time),month(p.entry_time),day(p.entry_time));



<h4>----Back up Comparission----</h4>
create table daily_txt ( cust_id int,username string,quote_count int,ip string,entry_time date,prp_1 int,prp_2 int,prp_3 int,ms int,http_type string,purchase_category string,total_count int,purchase_sub_category string,http_info string,status_code int,file_name string,year string,month string,day string) row format delimited fields terminated by ',';
insert overwrite table daily_txt select * from daily_ext;

<h3>----sqoop----</h3>

sqoop export  --connect jdbc:mysql://localhost:3306/testing?useSSL=False --username root --password Welcome@123  --table daily_txt --export-dir /user/hive/warehouse/cohort_c9.db/daily_txt  -m 1


<h2>---Truncate Table----</h2>

<h3>----sql----</h3>
Truncate table daily;
truncate table daily_bkp;

<h3>----hive----</h3>
Truncate table daily;






    

