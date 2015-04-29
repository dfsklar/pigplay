-- This is a "diff" operation.

NEW = LOAD 'statetoday' using PigStorage(',') as (user:chararray,code:chararray);
OLD = LOAD 'stateprev'  using PigStorage(',') as (user:chararray,code:chararray);

-- "GROUP BY" did not work as it produced a bag -- JOIN is good in that it flattens the items being joined
JOINME = JOIN OLD BY user FULL OUTER, NEW BY user;
-- EXAMPLE (showing two new-new users):
--(USER100,N,,)
--(USER101,N,USER101,N)
--(USER102,N,USER102,N)
--(USER103,N,USER103,N)
--(USER104,N,USER104,N)
--(USER105,N,USER105,N)
--(USER106,N,USER106,N)
--(USER107,N,,)
--(USER108,N,USER108,N)
--(USER109,N,USER109,N)

DELTAS = FILTER JOINME BY (OLD::code IS NULL) OR (OLD::code != NEW::code);
DUMP DELTAS;
--(,,USER100,N)
--(,,USER107,N)

-- At this point, we have in DELTAS one tuple for each user who "changed state".
-- Example: (sam,D,sam,A)
-- Example: (,,sam,N)

-- We are keenly interested in the *counts* of transitions in each direction, so
-- let's compute the distinct transitions and produce a count for each.

ALLTRANSITIONS = FOREACH DELTAS GENERATE CONCAT( (($1 is null) ? '-' : $1), $3) as transdirection;
TRANSTYPES = GROUP ALLTRANSITIONS BY transdirection;
COUNTOFTRANSTYPES = FOREACH TRANSTYPES GENERATE COUNT(ALLTRANSITIONS), group;

STORE COUNTOFTRANSTYPES INTO 'countTransitionTypes';

