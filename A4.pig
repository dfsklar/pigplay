-- This is our attempt at creating a derived table.

RAW = LOAD 'active_user_stats_normalized.txt' using PigStorage(',') as (dt:int,user:chararray,code:chararray);

-- If we want to ignore "U" and only look at playing, we would then:
--    RAWFILTERED = FILTER RAW BY code == "P";
--

PERUSER = GROUP RAW BY user;

PERUSERACTIVE = FILTER PERUSER BY COUNT(RAW) > 3;
COUNTPERUSERACTIVE = FOREACH PERUSERACTIVE GENERATE group, 'A';
PERUSERDORMANT = FILTER PERUSER BY COUNT(RAW) <= 3;
COUNTPERUSERDORMANT = FOREACH PERUSERDORMANT GENERATE group, 'D';

-- We now have divided users into two buckets and we're ready to dump a "NEWSTATE" table.
NEWSTATE = UNION COUNTPERUSERACTIVE, COUNTPERUSERDORMANT;

-- DUMP NEWSTATE;

-- Note that we would in the real world possibly store this in .gz format
STORE NEWSTATE INTO 'newstate2' USING PigStorage(',');

