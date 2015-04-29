
RAW = LOAD 'pigtable.txt' using PigStorage(',') as (dt:int,user:chararray,code:chararray);

PERUSER = GROUP RAW BY user;

COUNTPERUSER = FOREACH PERUSER GENERATE group, COUNT(RAW);

DUMP COUNTPERUSER;
