For Q1
Run the command like this:

hdfs dfs -rm -r -f output1
hdfs dfs -rm -r -f output2
hdfs dfs -rm -r -f output3
hadoop jar hw2p1.jar hw2p1 users ratings movies output1 output2 output3 output4
hadoop jar hw2p1.jar hw2p1 users ratings movies output1 output2 output3 output4



hdfs dfs -cat output4/*


For Q2
Run the command like this:

hdfs dfs -rm -r -f output
hadoop jar Q2.jar Q2 input/users.dat input/ratings.dat output 22
hdfs dfs -cat output/*