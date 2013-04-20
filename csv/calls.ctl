load data 
infile 'calls.csv' "str '\r\n'"
append
into table CALLS
fields terminated by ','
OPTIONALLY ENCLOSED BY '"' AND '"'
trailing nullcols
           ( CALLID CHAR(4000),
             DATEOFCALL DATE "yyyy-mm-dd",
             QUARTER CHAR(4000),
             YEAR CHAR(4000),
             STARTTIME DATE "null",
             ENDTIME DATE "null",
             COMPANYID CHAR(4000),
             COMPANYNAME CHAR(4000),
             OUTCOME CHAR(4000),
             CANSTOCKEX CHAR(4000),
             EXTRANUM CHAR(4000),
             EXTRACHAR CHAR(4000),
             EXTRALONGCHAR CHAR(4000),
             TITLE CHAR(4000)
           )
