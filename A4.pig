
-- Is this technique (LOAD then FILTER) as good as just loading the specific partitions needed?
-- Is the load really "deferred" until the filtered set is used?
-- I ask because if we are looking at just 7 days, we really
-- need only 14 of the partitions, considering the way active_user_stats is currently partitioned.
-- 
-- Currently, I ignore the "P" vs "U".  Thus the use of "DISTINCT" is sufficient to collapse
-- case "MULTIN1DAY" to a single tuple.  (Case MULTIN1DAY refers to the current active_user_stats
-- table's tendency to have multiples rows for one user on one day if multiple devices were used.)
RAWALLDATES = LOAD 'input_peruser_perday_uniq.txt' using PigStorage(',') as (dt:int,user:chararray);
RAW_NONUNIQ = FILTER RAWALLDATES BY (dt > ($today - $windowSizeNumDays)) AND (dt < $today);
RAW = DISTINCT RAW_NONUNIQ;  -- This is ok because we are ignoring the P/U code currently.

-- Each individual user will be examined individually and placed in one bucket.
PERUSER = GROUP RAW BY user;
SPLIT PERUSER INTO BUCKETACTIVEUSERS IF COUNT(RAW) >= $minAcceptableActivityInWindow, BUCKETDORMANTUSERS IF COUNT(RAW) < $minAcceptableActivityInWindow;
ACTIVEUSERS = FOREACH BUCKETACTIVEUSERS GENERATE group, 'A';  -- the 'group' is a userID
DORMANTUSERS = FOREACH BUCKETDORMANTUSERS GENERATE group, 'D'; -- the 'group' is a userID


-----------------------------------------
-- COUNT THE NUMBER IN EACH BUCKET
-- Surely there's a better way to do this?
-- 1) active
GallACTIVE = group ACTIVEUSERS all;
CountACTIVE = foreach GallACTIVE generate COUNT(ACTIVEUSERS);
STORE CountACTIVE INTO 'countActive' USING PigStorage(',');
-- 2) dormant
GallDORMANT = group DORMANTUSERS all;
CountDORMANT = foreach GallDORMANT generate COUNT(DORMANTUSERS);
STORE CountDORMANT INTO 'countDormant' USING PigStorage(',');


--------------------------------------------------------------
-- SAVE THE ENTIRE USER DATABASE (union of active and dormant)
--
NEWSTATE = UNION ACTIVEUSERS, DORMANTUSERS;
STORE NEWSTATE INTO 'statetoday' USING PigStorage(',');
