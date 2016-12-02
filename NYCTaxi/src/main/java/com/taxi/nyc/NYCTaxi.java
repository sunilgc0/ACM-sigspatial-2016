/**
 * Created by Sunil on 16-Nov-16.
 */
package com.taxi.nyc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


public class NYCTaxi implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NYC App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = "file:///C:\\test/yellow_fd.csv";
        //String inputPath = "file:///C:\\test/yellow_tripdata_2015-01.csv";
        //String inputPath = args[0];
        //String outputPath = args[1];
        JavaRDD<String> input = sc.textFile(inputPath);

        //Store the header to be removed
        String header = input.first();
        //Filter Predicate defines Function which removes using filter expression
        Function<String, Boolean> filterPredicate = line -> !line.contains(header);
        JavaRDD<String> withoutHeader = input.filter(filterPredicate);

        //Create Objects for each trip
        PairFunction<String, Integer, Float[]> mapLines = lines -> {
            String[] fields = lines.split(",");

            Date pickupDate = new Date();
            Float[] coOrdinate = new Float[2];;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                pickupDate = simpleDateFormat.parse(fields[1]);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            int  day = pickupDate.getDate();
            coOrdinate[0] = Float.valueOf(fields[6]);
            coOrdinate[1] = Float.valueOf(fields[5]);
            return new Tuple2<>(day, coOrdinate);
        };
        JavaPairRDD<Integer, Float[]> collection = withoutHeader.mapToPair(mapLines);

        //Filter trips which are not within new york and Pair Date with pickup location for each trip
        Function<Tuple2<Integer, Float[]>, Boolean> clipping = (Tuple2<Integer, Float[]> tuple2) -> {
            Float[] coOrd = tuple2._2();
            return(coOrd[0]>=40.5f && coOrd[0]<=40.9f && coOrd[1]>=-74.25f && coOrd[1]<=-73.7f);
        };
        JavaPairRDD<Integer, Float[]> pairRDD = collection.filter(clipping);

        //Get number of pickups for each location
        PairFunction<Tuple2<Integer, Float[]>,List<Integer>,Integer> getPickupCount = (Tuple2<Integer, Float[]> tuple2) -> {
            Integer day = tuple2._1();
            Float[] coOrd = tuple2._2();
            Float lat = coOrd[0];
            Float longi = coOrd[1];

            List<Integer> cellLocation = Boundary.getCellLocation(lat,longi,day);
            Integer count = 1;
            return new Tuple2<>(cellLocation,count);
        };
        JavaPairRDD<List<Integer>, Integer> LocationCount = pairRDD.mapToPair(getPickupCount);
       //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceLocation = (accum, n) -> (accum + n);
        JavaPairRDD<List<Integer>, Integer> reducedRDD = LocationCount.reduceByKey(reduceLocation);

        //It is sum of all pickups from all cells
        long sigmaX = LocationCount.count();
        System.out.println("Sum of all pickups from all cells "+ sigmaX);
        //It is list of all triple where some pickup happened
        List<Integer> Xlist = reducedRDD.values().collect();
        //RDD cannot be nested in other rdd map, so broadcast them and then use
        Broadcast<List<Tuple2<List<Integer>, Integer>>> broadcast = sc.broadcast(reducedRDD.collect());
        //Calculate mean for this dataset
        float mean = GetisCalculator.getMean(sigmaX);
        //Calculate Standard deviation for this dataset
        float SD = GetisCalculator.getSD(Xlist, mean);

        //Calculating getis ord for all cells
        PairFunction<Tuple2<List<Integer>,Integer>, Float,List<Integer>> getis = (Tuple2<List<Integer>,Integer> tuple2) -> {
            List<Integer> locTriple = tuple2._1();
            //neighborList is all potential neighbors i.e 27
            List<List<Integer>> neighborList = Boundary.NeighborList(locTriple);
            //sigmaList has actual verified neighbors
            List<Integer> sigmaList = Boundary.getSigmaList( broadcast.getValue(), neighborList, locTriple);

            float zscore= GetisCalculator.getScore(sigmaList, mean, SD);
            return new Tuple2<>(zscore, locTriple);
        };
        JavaPairRDD<Float, List<Integer>> getisValues = reducedRDD.mapToPair(getis);

        //Descending order of zscore
        JavaPairRDD<Float, List<Integer>> finalResult  = getisValues.sortByKey(false); //false for descending order

        //take top 50, from highest to decreasing values
        List<Tuple2<Float, List<Integer>>> top50 = finalResult.take(50);

        //Write results to file
        try {
            //File file = new File(outputPath);
            File file  = new File("C:\\test\\filename.txt");
            if (!file.exists()) file.createNewFile();
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            for(Tuple2 tuple2 : top50){
                List<Integer> coord = (List<Integer>)tuple2._2();
                Float zscore  = (Float) tuple2._1();
                bw.write(String.valueOf(coord.get(0)+", "+coord.get(1)+", "+coord.get(2)+", "+zscore+"\n"));
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Highest zscore is "+finalResult.first()._1() +" from location and time "+ finalResult.first()._2());
    }
}
