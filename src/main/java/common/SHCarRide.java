package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * @auther: zk
 * @date: 2022/3/24 15:26
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SHCarRide {
    public int carId;
    public Instant eventTime;
    public Instant processTime;
    public float lat;
    public float lon;
    public float angle;
    public float speed;


    /**
     * Gets the ride's time stamp as a long in millis since the epoch.
     */
    public long getEventTimeMillis() {
        return eventTime.toEpochMilli();
    }

    /**
     * Gets the ride's time stamp as a long in millis since the epoch.
     */
    public long getProcessTimeMillis() {
        return processTime.toEpochMilli();
    }
}

