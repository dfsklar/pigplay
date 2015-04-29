-- A6
--
-- This version tries to identify NEW users based on another input database of "birthdays",
-- differs from A5 in that I'm doing the join of birthdates later in the game.

birthdates = LOAD 'input_birthdates.txt' using PigStorage(',') as (user:chararray,birthdate:int);

--
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

-- At this point, each RAW tuple is a unique user+date permutation.  The user "showed up" on that date.
-- We are not distinguishing "intensity of activity" in this version of the system.

-- ILLUSTRATE rawPlusBirthdates;


-- Each individual user will be examined individually and placed in one bucket.
peruserBeforeBirthdates = GROUP RAW BY user;

ILLUSTRATE peruserBeforeBirthdates;

-- Let's fold in the birthdate information:
peruser = JOIN peruserBeforeBirthdates BY group LEFT OUTER, birthdates BY user;
ILLUSTRATE peruser;




-- DUMP peruser;

SPLIT peruser INTO 
  bucketNewUsers IF (birthdates::birthdate IS NULL) OR ( ($today - birthdate) < 5 ),
  bucketActiveUsers IF COUNT(rawPlusBirthdates) >= $minAcceptableActivityInWindow, 
  bucketDormantUsers IF COUNT(rawPlusBirthdates) < $minAcceptableActivityInWindow;

