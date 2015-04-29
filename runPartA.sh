/bin/rm -rf stateprev
TODAY=20150107
mv statetoday stateprev
pig -x local -param today=$TODAY -param windowSizeNumDays=7 -param minAcceptableActivityInWindow=3 A4.pig
mv countActive countActive_$TODAY
mv countDormant countDormant_$TODAY
