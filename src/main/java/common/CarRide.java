package common;

import common.utils.CarDataGenerator;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;

@NoArgsConstructor
@AllArgsConstructor
@ToString

public class CarRide {
    public Long carRideId;
    public String carId;
    public Instant eventTime;
    public float lat;
    public float lon;
    public float angle;
    public float speed;

    public CarRide(long carRideId){
        CarDataGenerator g = new CarDataGenerator(carRideId);
        this.carRideId = carRideId;
        this.carId = g.carId();
        this.eventTime = g.eventTime();
        this.lat = g.lat();
        this.lon = g.lon();
        this.angle = g.angle();
        this.speed = g.speed();
    }

    /** Gets the ride's time stamp as a long in millis since the epoch. */
    public long getEventTimeMillis() {
        return eventTime.toEpochMilli();
    }
}
