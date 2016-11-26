package com.taxi.nyc;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Sunil on 16-Nov-16.
 */
public class TaxiTrip implements Serializable{
    Date pickupDate;
    float distance;
    Float[] coOrdinate;

    public TaxiTrip(String pickupDate, String distance, String longitude, String latitude) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            this.pickupDate = simpleDateFormat.parse(pickupDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        this.distance = Float.valueOf(distance);
        this.coOrdinate = new Float[2];
        coOrdinate[0] = Float.valueOf(latitude);
        coOrdinate[1] = Float.valueOf(longitude);
    }
}
