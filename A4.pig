-- This is our attempt at creating a derived table.

RAWALLDATES = LOAD 'input_peruser_perday.txt' using PigStorage(',') as (dt:int,user:chararray,code:chararray);


RAW = FILTER RAWALLDATES BY (dt > ($today - $windowSizeNumDays)) AND (dt < $today);

-- If we want to ignore "U" and only look at playing, we would then:
--    RAWFILTERED = FILTER RAW BY code == "P";
--

PERUSER = GROUP RAW BY user;

SPLIT PERUSER INTO BUCKETACTIVEUSERS IF COUNT(RAW) >= $minAcceptableActivityInWindow, BUCKETDORMANTUSERS IF COUNT(RAW) < $minAcceptableActivityInWindow;
ACTIVEUSERS = FOREACH BUCKETACTIVEUSERS GENERATE group, 'A';
DORMANTUSERS = FOREACH BUCKETDORMANTUSERS GENERATE group, 'D';

-- We now have divided users into two buckets and we're ready to dump a "NEWSTATE" table.
NEWSTATE = UNION ACTIVEUSERS, DORMANTUSERS;

STORE NEWSTATE INTO 'statetoday' USING PigStorage(',');

-- None of this works:
--CCD = FOREACH ('D','A') GENERATE ('D', COUNT(DORMANTUSERS));
--CCA = ('A', COUNT(ACTIVEUSERS));
--CCC = UNION CCA, CCD;
--STORE CCC INTO 'countActive_'+$today USING PigStorage(',');

