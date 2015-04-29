-- This is a "diff" operation.

OLD = LOAD 'statetoday' using PigStorage(',') as (user:chararray,code:chararray);
NEW = LOAD 'stateprev'  using PigStorage(',') as (user:chararray,code:chararray);

-- "GROUP BY" did not work as it produced a bag -- JOIN is good in that it flattens the items being joined
JOINME = JOIN OLD BY user, NEW BY user;

DELTAS = FILTER JOINME BY NOT (OLD::code == NEW::code);

--ILLUSTRATE JOINME;

-- At this point, we have in DELTAS one tuple for each user who "changed state".
-- Example: (sam,D,sam,A)

-- We are keenly interested in the *counts* of transitions in each direction, so
-- let's compute the distinct transitions and produce a count for each.

XYZ = FOREACH DELTAS GENERATE CONCAT($1, $3);

DUMP XYZ;

