package source;

import common.CarRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @auther: zk
 * @date: 2021/12/28 15:08
 */
public class CarDataSource implements SourceFunction<CarRide> {
    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private volatile boolean flag = true;


    private Random random = new Random();

    @Override
    public void run(SourceContext<CarRide> ctx) throws Exception {
        long id = 0;
        while (flag) {

            List<CarRide> events = new ArrayList<CarRide>(BATCH_SIZE);
            for (int i = 1; i <= BATCH_SIZE; i++) {
                CarRide carRide = new CarRide(id + i);
                events.add(carRide);
            }

            java.util.Collections.shuffle(events, new Random(id));
            events.iterator().forEachRemaining(r -> ctx.collect(r));

            // prepare for the next batch
            id += BATCH_SIZE;

            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);

        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

}
