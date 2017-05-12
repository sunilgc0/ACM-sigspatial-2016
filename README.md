# ACM SIGSPATIAL Cup 2016
## Problem Definition 

 - Input: A collection of New York City Yellow Cab taxi trip records spanning January 2009 to June 2015. The source data may be clipped to an envelope encompassing the five New York City boroughs in order to remove some of the noisy error data (e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W).

 - Output: A list of the fifty most significant hot spot cells in time and space as identified using the Getis-Ord  statistic.
[![N|Solid](http://sigspatial2016.sigspatial.org/giscup2016/gfx/image004.png)](https://nodesource.com/products/nsolid)


## System Requirement:
* JDK version 1.8, compiled with JDK version 1.8.0_71
* compiled with Spark 2.0.1 version

## Guidelines
* Please use local file system location as input and output path, This is tested with files read and wrote to local file system. This was done as coalesce(1,true).saveAsTextFile() was not creating in the format specified.
* If you are using cluster, make sure the input file location is consistent across nodes.
* Set master URL if you running in cluster, so that it would distribute computation across nodes
* I am using input csv file with header, so please use input csv file with header

## How to run:
./spark-submit --master \<SPARK_MASTER_URL> --class com.taxi.nyc.NYCTaxi NYCTaxi.jar \<INPUT_FILE> \<OUPUT_FILE> 

EXAMPLE
./spark-submit --master spark://ip-172-31-32-141:7077 --class com.taxi.nyc.NYCTaxi NYCTaxi.jar /home/ubuntu/datasets/yellow_tripdata_2015-01.csv 	 	/home/ubuntu/datasets/output.csv
