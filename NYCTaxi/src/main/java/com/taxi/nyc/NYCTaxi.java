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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
/**
 * Created by Sunil on 16-Nov-16.
 */

@SuppressWarnings("all")
public class NYCTaxi implements Serializable {

    //Create RDD by getting required values from each line
    static PairFunction<String, Integer, Float[]> mapLines = lines -> {
        String[] fields = lines.split(",");

        Date pickupDate = new Date();
        Float[] coOrdinate = new Float[2];
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

    //Filter trips which are not within new york and Pair Date with pickup location for each trip
    static Function<Tuple2<Integer, Float[]>, Boolean> clipping = (Tuple2<Integer, Float[]> tuple2) -> {
        Float[] coOrd = tuple2._2();
        return(coOrd[0]>=40.5f && coOrd[0]<=40.9f && coOrd[1]>=-74.25f && coOrd[1]<=-73.7f);
    };

    //Get number of pickups for each location
    static PairFunction<Tuple2<Integer, Float[]>,List<Integer>,Integer> getPickupCount = (Tuple2<Integer, Float[]> tuple2) -> {
        Integer day = tuple2._1();
        Float[] coOrd = tuple2._2();
        Float lat = coOrd[0];
        Float longi = coOrd[1];
        List<Integer> cellLocation = Boundary.getCellLocation(lat,longi,day);
        Integer count = 1;
        return new Tuple2<>(cellLocation,count);
    };

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("NYC Taxi Hotspot App");
        //SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NYC Taxi Hotspot App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        JavaRDD<String> input = sc.textFile(inputPath);

        //Store the header to be removed
        String header = input.first();
        //Filter Predicate defines Function which removes using filter expression
        Function<String, Boolean> filterPredicate = line -> !line.contains(header);
        JavaRDD<String> withoutHeader = input.filter(filterPredicate);

        //Create RDD by getting required values from each line
        JavaPairRDD<Integer, Float[]> collection = withoutHeader.mapToPair(mapLines);

        //Filter trips which are not within new york and Pair Date with pickup location for each trip
        JavaPairRDD<Integer, Float[]> pairRDD = collection.filter(clipping);

        //Get number of pickups for each location
        JavaPairRDD<List<Integer>, Integer> LocationCount = pairRDD.mapToPair(getPickupCount);
        //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceLocation = (accum, n) -> (accum + n);
        JavaPairRDD<List<Integer>, Integer> reducedRDD = LocationCount.reduceByKey(reduceLocation);

        //It is list of all triple where some pickup happened
        List<Integer> Xlist = reducedRDD.values().collect();
        //It is sum of all pickups from all cells
        long sigmaX = 0; // LocationCount.count();
        for(Integer x : Xlist){
            sigmaX = sigmaX + x;
        }
        //RDD cannot be nested in other rdd map, so broadcast them and then use
        Broadcast<List<Tuple2<List<Integer>, Integer>>> broadcast = sc.broadcast(reducedRDD.collect());
        //Calculate mean for this dataset
        double mean = GetisCalculator.getMean(sigmaX);
        //Calculate Standard deviation for this dataset
        double SD = GetisCalculator.getSD(Xlist, mean);

        //Calculating getis ord for all cells
        PairFunction<Tuple2<List<Integer>,Integer>, Double,List<Integer>> getis = (Tuple2<List<Integer>,Integer> tuple2) -> {
            List<Integer> locTriple = tuple2._1();
            //neighborList is all potential neighbors i.e 27
            List<List<Integer>> neighborList = Boundary.NeighborList(locTriple);
            //sigmaList has actual verified neighbors
            List<Integer> sigmaList = Boundary.getSigmaList( broadcast.getValue(), neighborList, locTriple);

            double zscore= GetisCalculator.getScore(sigmaList, mean, SD);
            return new Tuple2<Double,List<Integer>>(zscore, locTriple);
        };
        JavaPairRDD<Double, List<Integer>> getisValues = reducedRDD.mapToPair(getis);

        //Descending order of zscore, use takeOrdered for faster, sortbyKey is slower
        List<Tuple2<Double, List<Integer>>> top50 = getisValues.takeOrdered(50, TupleComparator.INSTANCE);

        //Write results to file
        writeToFile(top50, outputPath);

//        JavaPairRDD<Double, List<Integer>> writeRDD = sc.parallelizePairs(top50);
//        writeRDD.coalesce(1,true).saveAsTextFile(outputPath);
    }

    static class TupleComparator implements Comparator<Tuple2<Double, List<Integer>>>,Serializable {
        final static TupleComparator INSTANCE = new TupleComparator();
        public int compare(Tuple2<Double, List<Integer>> t1, Tuple2<Double, List<Integer>> t2) {
            return -t1._1.compareTo(t2._1);     // sorts RDD elements descending
        }
    }

    static void writeToFile(List<Tuple2<Double, List<Integer>>> top50, String outputPath){
        try {
            File file  = new File(outputPath);
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            for(Tuple2 tuple2 : top50){
                List<Integer> coord = (List<Integer>)tuple2._2();
                Double zscore  = (Double) tuple2._1();
                bw.write(String.valueOf(coord.get(0)+","+coord.get(1)+","+coord.get(2)+","+zscore+"\n"));
            }
            bw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Highest zscore is "+top50.get(0)._1() +" from location and time "+ top50.get(0)._2());
    }
}
