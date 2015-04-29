/bin/rm -rf stateprev
mv statetoday stateprev
pig -x local -param today=20150104 -param windowSizeNumDays=7 -param minAcceptableActivityInWindow=3 A4.pig
