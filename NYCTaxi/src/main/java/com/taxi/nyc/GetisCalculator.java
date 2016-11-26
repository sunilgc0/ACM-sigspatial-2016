package com.taxi.nyc;

import scala.Serializable;
import java.util.List;

/**
 * Created by Sunil on 22-Nov-16.
 */
public class GetisCalculator implements Serializable {
    public static int spatialWeight = 1;
    public static int cellCount = 68200;

    public static float getMean(float sigmaAttr){
        return sigmaAttr/cellCount;
    }

    public static float getSD(List<Integer> sigmaList, float mean){
        long sum =0;
        for(Integer item : sigmaList){
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

    public static float getScore(List<Integer> list, int pickupCount, List<List<Integer>> neighList, int neighborcount,
                                 List<Integer> sigmaList, float sigmaAttr){
        float numerator = sigmaAttr*(1-(neighborcount/(float)cellCount));
        float mean = getMean(sigmaAttr);
        float SD = getSD(sigmaList, mean);
        float det = getDet(neighborcount);
        float denominator = SD * det;

        float zscore = numerator/denominator;

        return zscore;
    }



}
