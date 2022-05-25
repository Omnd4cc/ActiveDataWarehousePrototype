package ActiveDataWarehouse;

import common.CarRide;
import common.SHCarRide;
import operator.DynamicKeyFunction;
import operator.DynamicQueryFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import rule.Rule;
import rule.RuleDeserializer;
import source.CarDataFromFiles;
import source.CarDataSource;
import source.ShCarDataDeserializer;

import java.util.concurrent.TimeUnit;

/**
 * @author zk
 * @date: 2021/3/24 16:20
 */
public class App {
    public static void main(String[] args) throws Exception {
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("rules")
                //Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setProperty("enable.auto.commit", "true")
                .setGroupId("rules")
                .build();

//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).setParallelism(1);

        DataStreamSource<String> ruleSourceStream = env.fromSource(ruleSource, WatermarkStrategy.noWatermarks(),"kafka Source");

        DataStream<Rule> getRulesUpdateStream = ruleSourceStream
                .returns(Types.STRING)
                .flatMap(new RuleDeserializer())
                .name("Rule Deserialization");


        BroadcastStream<Rule> rulesStream = getRulesUpdateStream.broadcast(DynamicKeyFunction.Descriptors.rulesDescriptor);

        ruleSourceStream.print();
        DataStream<SHCarRide> shCarRideSource= env
                .addSource(new CarDataFromFiles())
                .flatMap(new ShCarDataDeserializer())
                .name("CarData Deserialization");

//        shCarRideSource.print();

        DataStream alerts =  shCarRideSource
                .connect(rulesStream)
                .process(new DynamicKeyFunction())
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                .keyBy((keyed) -> keyed.getKey())
                .connect(rulesStream)
                .process(new DynamicQueryFunction());

//        DataStream<String> allRuleEvaluations =
//                ((SingleOutputStreamOperator<Rule>) alerts).getSideOutput(DynamicKeyFunction.Descriptors.demoSinkTag);
//        allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

//        DataStream<Long> lagenctyData =
//                ((SingleOutputStreamOperator<Rule>) alerts).getSideOutput(DynamicKeyFunction.Descriptors.latencySinkTag);
//        lagenctyData.print().name("lagency Sink");
        env.execute();

    }
}
