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


    

