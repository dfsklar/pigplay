TODAY=20150107
pig -x local B1.pig || exit
/bin/rm -rf             countTransitionTypes_$TODAY
mv countTransitionTypes countTransitionTypes_$TODAY
