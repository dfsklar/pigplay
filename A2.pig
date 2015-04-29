-- This is a start at step "A", ie. generating the new state
RAW = LOAD 'pigtable.txt' using PigStorage(',') as (dt:int,user:chararray,code:chararray);

-- If we want to ignore "U" and only look at playing, we would then:
--    RAWFILTERED = FILTER RAW BY code == "P";
--

PERUSER = GROUP RAW BY user;

PERUSERACTIVE = FILTER PERUSER BY COUNT(RAW) > 3;

COUNTPERUSERACTIVE = FOREACH PERUSERACTIVE GENERATE group, COUNT(RAW);

DUMP COUNTPERUSERACTIVE;
-- (dfs,4)

