package common.utils;

import java.time.Instant;
import java.util.Random;

/**
 * @auther: zk
 * @date: 2021/12/28 10:10
 */
public class CarDataGenerator {

    private static final String[] CAR_ID = {"K1019", "K1018", "K1017", "K1016", "K1015", "K1014", "K1013", "K1012", "K1011", "K1010",
            "K2019", "K2018", "K2017", "K2016", "K2015", "K2014", "K2013", "K2012", "K2011", "K2010"};
    private transient long carRideId;

    /**
     * Creates a DataGenerator for the specified rideId.
     */
    public CarDataGenerator(long carRideId) {
        this.carRideId = carRideId;
    }

    public String carId() {
        long ind = (carRideId%CAR_ID.length);
        int index =  (int)ind;
        return CAR_ID[index];
    }

    /**
     * Deterministically generates and returns the eventTime for this ride.
     */
    public Instant eventTime() {
        return Instant.now();
    }

    public float lat() {
        return aFloat((float) (common.utils.GeoUtils.LAT_SOUTH - 0.1), (float) (common.utils.GeoUtils.LAT_NORTH + 0.1F));
    }

    /**
     * Deterministically generates and returns the startLon for this ride.
     */
    public float lon() {
        return aFloat((float) (common.utils.GeoUtils.LON_WEST - 0.1), (float) (common.utils.GeoUtils.LON_EAST + 0.1F));
    }

    public float angle() {
        Random random = new Random();
        return random.nextFloat() * 360;
    }

    public float speed() {
        Random random = new Random();
        return random.nextFloat() * 150;
    }

    // -------------------------------------

    private long aLong(long min, long max) {
        float mean = (min + max) / 2.0F;
        float stddev = (max - min) / 8F;

        return aLong(min, max, mean, stddev);
    }

    // the rideId is used as the seed to guarantee deterministic results
    private long aLong(long min, long max, float mean, float stddev) {
        Random rnd = new Random(carRideId);
        long value;
        do {
            value = (long) Math.round((stddev * rnd.nextGaussian()) + mean);
        } while ((value < min) || (value > max));
        return value;
    }

    // -------------------------------------

    private float aFloat(float min, float max) {
        float mean = (min + max) / 2.0F;
        float stddev = (max - min) / 8F;

        return aFloat(carRideId, min, max, mean, stddev);
    }

    private float bFloat(float min, float max) {
        float mean = (min + max) / 2.0F;
        float stddev = (max - min) / 8F;

        return aFloat(carRideId + 42, min, max, mean, stddev);
    }

    // the rideId is used as the seed to guarantee deterministic results
    private float aFloat(long seed, float min, float max, float mean, float stddev) {
        Random rnd = new Random(seed);
        float value;
        do {
            value = (float) (stddev * rnd.nextGaussian()) + mean;
        } while ((value < min) || (value > max));
        return value;
    }
}



