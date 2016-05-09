# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

INPUT=/shared/HW2/sample-in/input-100M
OUTPUT=/user/s101062105/hw2/output
TMP_OUTPUT=/user/s101062105/hw2/tmp_output

hdfs dfs -rm -r $OUTPUT
hdfs dfs -rm -r $TMP_OUTPUT
hadoop jar PageRank.jar pageRank.PageRank $INPUT $OUTPUT
hdfs dfs -cat ${OUTPUT}/part-* > output.log
