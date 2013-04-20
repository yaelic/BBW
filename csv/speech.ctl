load data 
infile 'speech.csv' "str '\r\n'"
append
into table SPEECH
fields terminated by ','
OPTIONALLY ENCLOSED BY '"' AND '"'
trailing nullcols
           ( SPEECHID CHAR(4000),
             CALLID CHAR(4000),
             SPEECHSEGMENT CHAR(4000),
             SPEAKERID CHAR(4000),
             INDEXINCALL CHAR(4000),
             SPEECHPART CHAR(4000),
             LENGTH CHAR(4000),
             STARTTIME DATE "null",
             ENDTIME DATE "null",
             POSORNEG CHAR(4000),
             EMOTIONTAG CHAR(4000),
             EXTRA CHAR(4000),
             TYPE CHAR(4000),
             DATA CHAR(4000)
           )
