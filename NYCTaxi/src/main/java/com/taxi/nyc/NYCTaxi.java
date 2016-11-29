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
import java.util.Date;
import java.util.List;


public class NYCTaxi implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NYC App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String path = "file:///C:\\test/yellow_fd.csv";
        //String path = "file:///C:\\test/yellow_tripdata_2015-01.csv";
        String inputPath = args[0];
        String outputPath = args[1];
        JavaRDD<String> input = sc.textFile(inputPath);

        //Store the header to be removed
        String header = input.first();
        //Filter Predicate defines Function which removes using filter expression
        Function<String, Boolean> filterPredicate = line -> !line.contains(header);
        JavaRDD<String> withoutHeader = input.filter(filterPredicate);

        //Create Objects for each trip
        Function<String, TaxiTrip> mapLines = lines -> {
            String[] fields = lines.split(",");
            TaxiTrip taxiTrip = new TaxiTrip(fields[1], fields [4], fields[5], fields[6]);
            return taxiTrip;
        };
        JavaRDD<TaxiTrip> collection = withoutHeader.map(mapLines);

        //Filter trips which are not within new york
        Function<TaxiTrip, Boolean> clipping = (taxiTrip) -> {
            float lat = taxiTrip.coOrdinate[0];
            float lon = taxiTrip.coOrdinate[1];
            return(lat>=40.5f && lat<=40.9f && lon>=-74.25f && lon<=-73.7f);
        };
        JavaRDD<TaxiTrip> clippedArea = collection.filter(clipping);

        //Pair Date with pickup location for each trip
        PairFunction<TaxiTrip, Integer, Float[]> pairFunction = (TaxiTrip taxiTrip) -> {
            Date date = taxiTrip.pickupDate;
            int  day = date.getDate();
            Float[] coOrd = taxiTrip.coOrdinate;
            return new Tuple2<>(day, coOrd);
        };
        JavaPairRDD<Integer, Float[]> pairRDD  = clippedArea.mapToPair(pairFunction);

        //Get number of pickups for each location
        PairFunction<Tuple2<Integer, Float[]>,List<Integer>,Integer> getPickupCount = (Tuple2<Integer, Float[]> tuple2) -> {
            Integer day = tuple2._1();
            Float[] coOrd = tuple2._2();
            Float lat = coOrd[0];
            Float longi = coOrd[1];

            List<Integer> cellLocation = Boundary.getCellLocation(lat,longi,day);
            Integer count = Integer.valueOf(1);
            return new Tuple2<>(cellLocation,count);
        };
        JavaPairRDD<List<Integer>, Integer> Locationcount = pairRDD.mapToPair(getPickupCount);
       //Reduce Function for sum
        Function2<Integer, Integer, Integer> reduceLocation = (accum, n) -> (accum + n);
        JavaPairRDD<List<Integer>, Integer> reducedRDD = Locationcount.reduceByKey(reduceLocation);

        //It is sum of all pickups from all cells
        long sigmaX = Locationcount.count();
        //It is list of all triple where some pickup happened
        List<Integer> Xlist = reducedRDD.values().collect();
        //RDD cannot be nested in other rdd map, so broadcast them and then use
        Broadcast<List<Tuple2<List<Integer>, Integer>>> broadcast = sc.broadcast(reducedRDD.collect());
        //Calculating getis ord for all cells
        PairFunction<Tuple2<List<Integer>,Integer>, Float,List<Integer> > getis = (Tuple2<List<Integer>,Integer> tuple2) -> {
            List<Integer> locTriple = tuple2._1();
            //neighborList is all potential nighbors i.e 27
            List<List<Integer>> neighborList = Boundary.NeighborList(locTriple);
            //sigmaList has actual verified neighbors
            List<Integer> sigmaList = Boundary.getSigmaList( broadcast.getValue(), neighborList);
            //sum of all neighbors only
            float sigmaAttr = Boundary.getSumNeighbors(sigmaList);
            float zscore= GetisCalculator.getScore(sigmaList, sigmaAttr, sigmaX, Xlist);
            return new Tuple2<>(zscore, locTriple);
        };
        JavaPairRDD<Float, List<Integer>> getisValues = reducedRDD.mapToPair(getis);

        //Descending order of zscore
        JavaPairRDD<Float, List<Integer>> finalResult  = getisValues.sortByKey(false); //false for descending order

        //take top 50, from highest to decreasing values
        List<Tuple2<Float, List<Integer>>> top50 = finalResult.take(50);

        //Write results to file
        try {
            File file = new File(outputPath);
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
