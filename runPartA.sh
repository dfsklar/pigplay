TODAY=20150104
/bin/rm -rf stateprev
mv statetoday stateprev
/bin/rm -rf countActive countActive_$TODAY countDormant countDormant_$TODAY countNew countNew_$TODAY output_birthdates
pig -x local -param today=$TODAY -param windowSizeNumDays=7 -param minAcceptableActivityInWindow=3 -param newDuration=5 A6.pig
mv countActive/part-r-00000 countActive_$TODAY
mv countNew/part-r-00000 countNew_$TODAY
mv countDormant/part-r-00000 countDormant_$TODAY
/bin/rm -rf input_birthdates
mv output_birthdates input_birthdates

