H=/usr/local/hadoop/share/hadoop
javac -cp $H/hdfs/*:$H/mapreduce/*:$H/common/*:$H/yarn/*:/usr/lib/pig/pig-0.14.0/build/pig-0.14.0-SNAPSHOT.jar:$H/common/lib/*    com/yahoo/pigudf/Sim*.java

jar cvf0 ../SimpleTextStorer.jar com/yahoo/pigudf/SimpleTextStorer.class

