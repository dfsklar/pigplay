TODAY=20150104
/bin/rm -rf stateprev
mv statetoday stateprev
/bin/rm -rf countActive countActive_$TODAY countDormant countDormant_$TODAY
pig -x local -param today=$TODAY -param windowSizeNumDays=7 -param minAcceptableActivityInWindow=3 A4.pig
mv countActive/part-r-00000 countActive_$TODAY
mv countDormant/part-r-00000 countDormant_$TODAY
