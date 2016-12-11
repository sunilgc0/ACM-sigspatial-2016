package com.taxi.nyc;

import scala.Serializable;
import java.util.List;

/**
 * Created by Sunil on 22-Nov-16.
 */
@SuppressWarnings({"all"})
public class GetisCalculator implements Serializable {
    public static int cellCount = Boundary.cellCount();

    public static double getMean(float sigmaX){
        return sigmaX/cellCount;
    }

    public static double getSD(List<Integer> Xlist, double mean){
        long sum =0;
        for(Integer item : Xlist){
            sum = sum + item * item;
        }
        float div = sum / (float) cellCount;
        double diff = (div - (mean *mean));
        float SD = (float)Math.sqrt(diff);
        return SD;
    }

    public static float getDet(int neighborCount){
        int x = cellCount * neighborCount;
        int y  = neighborCount * neighborCount;
        int diff = x-y;
        float div =diff/((float)(cellCount-1));
        float result = (float)Math.sqrt(div);
        return result;
    }

    public static double getScore(List<Integer> sigmaList, double mean, double SD){
        int neighborCount = sigmaList.size();
        //sum of all neighbors in sigmaList
        float sigmaSum = Boundary.getSumNeighbors(sigmaList);
        double numerator = sigmaSum - (mean * neighborCount);
        float det = getDet(neighborCount);
        double denominator = SD * det;

        double zscore = numerator/denominator;
        return zscore;
    }
}