package com.taxi.nyc;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sunil on 22-Nov-16.
 */
public class Boundary  implements Serializable {
    public static final float latLow = 40.5f;
    public static final float latHigh = 40.9f;
    public static final float longLow = 73.7f;
    public static final float longHigh = 74.25f;
    public static final float coOrdStep =0.01f;
    public static final float timeStep = 1;
    public static int latAxis = (int) ((latHigh - latLow)/coOrdStep);
    public static int longAxis = (int) ((longHigh - longLow)/coOrdStep);

    public static int cellCount(){
        int timeAxis = 31;
        return latAxis*longAxis*timeAxis;    //68200
    }

    public static List<Integer> getCellLocation(float latInput, float longInput, int date){
        List<Integer> list = new ArrayList<>();
        int lat = (int)((latInput)*100);
        int longi = (int)((longInput)*100);
        int day = date - 1;
        list.add(lat);
        list.add(longi);
        list.add(day);
        return list;
    }

    public static List<List<Integer>> NeighborList(List<Integer> list) {
        int lat = list.get(0);
        int lon = list.get(1);
        int day = list.get(2);

        List<List<Integer>> neighbor = new ArrayList<>();

        int[] arr={-1, 0, 1};
        for(int i : arr){
            for(int j :arr){
                for(int k: arr){
                    List<Integer> result = new ArrayList<>();
                    result.add(0,lat + i);
                    result.add(1,lon + j);
                    result.add(2,day + k);
                    neighbor.add(result);
                }
            }
        }
        return neighbor;
    }

    public static int getSumNeighbors(List<Integer> neighborList){
        int sum=0;
        for(Integer item: neighborList){
                sum = sum + item;
        }
        return sum;
    }

    public static List<Integer> getSigmaList(List<Tuple2<List<Integer>, Integer>> pairRDD, List<List<Integer>> neighborList){
        List<Integer> sigmaList = new ArrayList<>();
        for(List<Integer> list: neighborList){
            for(Tuple2 t : pairRDD){
                List<Integer> key = (List<Integer>)t._1();
                if(key.equals(list)){
                    Integer sum = (Integer)t._2();
                    sigmaList.add(sum);
                }
            }
        }
        return sigmaList;
    }

}
