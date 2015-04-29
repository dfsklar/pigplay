-- This is a start at step "A", ie. generating the new state
RAW = LOAD 'pigtable.txt' using PigStorage(',') as (dt:int,user:chararray,code:chararray);

PERUSER = GROUP RAW BY user;

COUNTPERUSER = FOREACH PERUSER GENERATE group, COUNT(RAW);

DUMP COUNTPERUSER;
-- (dfs,4)
-- (sam,3)

