package source;

import common.SHCarRide;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @auther: zk
 * @date: 2022/3/24 16:25
 */
public class ShCarDataDeserializer extends RichFlatMapFunction<String, SHCarRide> {
    private CarDataParser carDataParser;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        carDataParser = new CarDataParser();
    }

    @Override
    public void flatMap(String value, Collector<SHCarRide> out) throws Exception {
        if (value.length() > 0) {
            SHCarRide ride = carDataParser.fromString(value);
            out.collect(ride);
        }
    }
}

