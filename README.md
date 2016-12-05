System Requirement:
JDK version 1.8, compiled with JDK version 1.8.0_71
compiled with Spark 2.0.1 version


1) Please use local file system location as input and output path, This is tested with files read and wrote to local file system.
This was done as coalesce(1,true).saveAsTextFile() was not creating in the format specified.
2) If you are using cluster, make sure the input file location is consistent across nodes.
3) Set master URL if you running in cluster, so that it would distribute computation across nodes
4) I am using input csv file with header, so please use input csv file with header

How to run:
./spark-submit --master <SPARK_MASTER_URL> --class com.taxi.nyc.NYCTaxi NYCTaxi.jar <INPUT_FILE> <OUPUT_FILE> 

EXAMPLE
./spark-submit --master spark://ip-172-31-32-141:7077 --class com.taxi.nyc.NYCTaxi NYCTaxi.jar /home/ubuntu/datasets/yellow_tripdata_2015-01.csv 	 	/home/ubuntu/datasets/output.csv