TODAY=20150107
/bin/rm -rf countTransitionTypes
pig -x local B1.pig || exit
/bin/rm -rf                          countTransitionTypes_$TODAY
mv countTransitionTypes/part-r-00000 countTransitionTypes_$TODAY
