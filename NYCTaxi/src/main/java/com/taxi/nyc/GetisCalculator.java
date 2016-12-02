package com.taxi.nyc;

import scala.Serializable;
import java.util.List;

/**
 * Created by Sunil on 22-Nov-16.
 */
@SuppressWarnings({"all"})
public class GetisCalculator implements Serializable {
    public static int cellCount = Boundary.cellCount();

    public static float getMean(float sigmaX){
        return sigmaX/cellCount;
    }

    public static float getSD(List<Integer> Xlist, float mean){
        long sum =0;
        for(Integer item : Xlist){
            sum = sum + item * item;
        }
        float div = sum / (float) cellCount;
        float diff = (div - (mean *mean));
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

    public static float getScore(List<Integer> sigmaList, float mean, float SD){
        int neighborCount = sigmaList.size();
        //sum of all neighbors in sigmaList
        float sigmaSum = Boundary.getSumNeighbors(sigmaList);
        float numerator = sigmaSum - (mean * neighborCount);
        float det = getDet(neighborCount);
        float denominator = SD * det;

        float zscore = numerator/denominator;
        return zscore;
    }
}