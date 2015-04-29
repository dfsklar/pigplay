-- A6
--
-- This version tries to identify NEW users based on another input database of "birthdays",
-- differs from A5 in that I'm doing the join of birthdates later in the game.

birthdates = LOAD 'input_birthdates' using PigStorage(',') as (user:chararray,birthdate:int);

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

-- ILLUSTRATE peruserBeforeBirthdates;

-- Let's fold in the birthdate information:
peruser = JOIN peruserBeforeBirthdates BY group LEFT OUTER, birthdates BY user;
-- ILLUSTRATE peruser;




-- DUMP peruser;



-- Note: SPLIT is not like a switch... an item can end up being placed in any number of the buckets.
-- So to achieve mutex, one must fully specify conditions, not assume "ELSE".

DEFINE isNew(b) RETURNS result {
  $result = (b IS NULL) OR ( ($today - b) < 5 );
};  -- MACRO IS NOT WORKING, THIS IS BEGIN IGNORED. CHECK INTO "INLINE" MACROS.

SPLIT peruser INTO 
  bucketNewUsers IF (birthdates::birthdate IS NULL) OR ( ($today - birthdates::birthdate) < $newDuration ),
  bucketActiveUsers IF NOT ((birthdates::birthdate IS NULL) OR ( ($today - birthdates::birthdate) < $newDuration )) AND (COUNT(peruserBeforeBirthdates::RAW) >= $minAcceptableActivityInWindow),
  bucketDormantUsers IF NOT ((birthdates::birthdate IS NULL) OR ( ($today - birthdates::birthdate) < $newDuration )) AND (COUNT(peruserBeforeBirthdates::RAW) < $minAcceptableActivityInWindow);

newUsers = FOREACH bucketNewUsers GENERATE group AS user, 'N' AS statecode;  -- the 'group' is a userID
activeUsers = FOREACH bucketActiveUsers GENERATE group AS user, 'A' AS statecode;  -- the 'group' is a userID
dormantUsers = FOREACH bucketDormantUsers GENERATE group AS user, 'D' AS statecode; -- the 'group' is a userID


--------------------------------------------------------------
-- SAVE THE ENTIRE USER DATABASE (union of active and dormant)
--
newState = UNION activeUsers, dormantUsers, newUsers;
STORE newState INTO 'statetoday' USING PigStorage(',');

-- SAVE A FRESH BIRTHDATE DATABASE
-- newUsers:  TUPLE (userid, 'N')
-- birthdates: TUPLE (userid, birthdate)
-- We want to add to birthdates any user who is not already represented therein, with $today as the birthdate.
-- If we do a JOIN of newUsers to birthdates, we'll get something where:
--    existing new users will have:  (userid, birthdate, 'N')
--    new new users will have:       (userid, null, 'N')
-- So at that point, we can do a FOREACH and fill in the null?
-- DUMP newUsers;

newBirthDB = JOIN birthdates BY user FULL OUTER, newUsers BY user;

-- The result is now:
--(,,USER101,N)
--(,,USER102,N)
--(,,USER103,N)
--(,,USER104,N)
--(,,USER105,N)
--(,,USER106,N)
--(,,USER108,N)
--(,,USER109,N)
--(DUMMYUSER,20100101,,)
--
-- One last FOREACH should suffice to finish this "merge":
newBirthDatabase = FOREACH newBirthDB GENERATE (($0 is null) ? $2 : $0) AS user, (($1 is null) ? $today : $1) AS birthdate;
STORE newBirthDatabase INTO 'output_birthdates' USING PigStorage(',');


------------------------------------------
--- COUNT THE NUMBER IN EACH BUCKET AND STORE THESE COUNTS
--- Surely there's a better way to do this?
--- 1) active
gallActive = GROUP activeUsers ALL;
countActive = FOREACH gallActive GENERATE COUNT(activeUsers);
STORE countActive INTO 'countActive' USING PigStorage(',');
-- 2) dormant
gallDormant = GROUP dormantUsers ALL;
countDormant = FOREACH gallDormant GENERATE COUNT(dormantUsers);
STORE countDormant INTO 'countDormant' USING PigStorage(',');
-- 3) new
gallNew = GROUP newUsers ALL;
countNew = FOREACH gallNew GENERATE COUNT(newUsers);
STORE countNew INTO 'countNew' USING PigStorage(',');
