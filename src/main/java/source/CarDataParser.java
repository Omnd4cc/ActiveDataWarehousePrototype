package source;

import common.SHCarRide;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @auther: zk
 * @date: 2022/3/24 16:27
 */
public class CarDataParser {

    private final ObjectMapper objectMapper = new ObjectMapper();


    public SHCarRide fromString(String carString) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat nowDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        List<String> tokens = Arrays.asList(carString.split(","));
//        System.out.println(tokens);
        Iterator<String> iter = tokens.iterator();

        SHCarRide ride = new SHCarRide();

        ride.setCarId(Integer.parseInt(stripBrackets(iter.next())));

        //获取事件时间
        String dateStr = stripBrackets((iter.next()));
        try {
            Date date = dateFormat.parse(dateStr);
            Instant instant = date.toInstant().plusMillis(TimeUnit.HOURS.toMillis(8));
            ride.setEventTime(instant);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //获取处理时间
        ride.setProcessTime(Instant.parse(stripBrackets((iter.next()))));
        //经度
        ride.setLon(Float.parseFloat(stripBrackets((iter.next()))));
        //纬度
        ride.setLat(Float.parseFloat(stripBrackets((iter.next()))));
        //速度
        ride.setSpeed(Float.parseFloat(stripBrackets((iter.next()))));
        //角度
        ride.setAngle(Float.parseFloat(stripBrackets((iter.next()))));

        return ride;

    }



    private static String stripBrackets(String expression) {
        return expression.replaceAll("[()]", "");
    }

    private static List<String> getNames(String expression) {
        String keyNamesString = expression.replaceAll("[()]", "");
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split("&", -1);
            return Arrays.asList(tokens);
        } else {
            return new ArrayList<>();
        }
    }
}
