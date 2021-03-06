package operator;

import common.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import rule.Rule;

import rule.Rule.ControlType;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static common.ProcessingUtils.handleRuleBroadcast;
import static rule.Rule.LimitOperatorType.EQUAL;

/**
 * @auther: zk
 * @date: 2021/12/22 15:13
 */
@Slf4j
public class DynamicKeyFunction extends BroadcastProcessFunction<SHCarRide, Rule, Keyed<SHCarRide, String, Long>> {
    private RuleCounterGauge ruleCounterGauge;

    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
    }

    @Override
    public void processElement(
            SHCarRide event, ReadOnlyContext ctx, Collector<Keyed<SHCarRide, String, Long>> out)
            throws Exception {
        ReadOnlyBroadcastState<Long, Rule> rulesState =
                ctx.getBroadcastState(Descriptors.rulesDescriptor);
        forkEventForEachGroupingKey(event, rulesState, out);
    }

    private void forkEventForEachGroupingKey(
            SHCarRide event,
            ReadOnlyBroadcastState<Long, Rule> rulesState,
            Collector<Keyed<SHCarRide, String, Long>> out)
            throws Exception {
        int ruleCounter = 0;
        for (Map.Entry<Long, Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue();
            List<WindowFilterRules> filterRules = rule.getWindowFilterRules();
            boolean outFlag = true;
            //?????????????????????????????????????????????????????????????????????throttle??????DELETE????????????
            if (rule.getActiveTime() != null && rule.getActiveTime() < System.currentTimeMillis() && rule.getLastTime() > 0) {
                rule.setQueryState(Rule.RuleState.DELETE);
                KafkaSender sender = KafkaSender.getInstance();
                sender.sendRule(rule, null);
            }

            //????????????????????????
            for (WindowFilterRules filterRule : filterRules) {
                //??????Rule POJO????????? apply??????????????????????????????BigDecimal????????????

                //?????????????????????????????????????????????
                Field field = event.getClass().getDeclaredField(filterRule.getField());
                //????????????????????????????????????=???????????????????????????=??????????????????????????????String???????????????
                Rule.LimitOperatorType operatorType = Rule.LimitOperatorType.fromString(filterRule.getOperator());
                if (operatorType == EQUAL) {
                    String ft = String.valueOf(field.get(event));
                    outFlag = ft.equals(filterRule.getValue());
                } else {
                    BigDecimal value = new BigDecimal(filterRule.getValue());
                    BigDecimal sourceValue = new BigDecimal(String.valueOf(field.get(event)));
//                    System.out.println(sourceValue);
//                    System.out.println(operatorType);
//                    System.out.println(value);
//                    System.out.println(applyCompare(sourceValue, operatorType, value));
                    if (!applyCompare(sourceValue, operatorType, value)) {
                        outFlag = false;
                        return;
                    }

                }


            }
            if (outFlag) {
                out.collect(
                        new Keyed<>(
                                event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getQueryId()));
                ruleCounter++;
            }


        }
        ruleCounterGauge.setValue(ruleCounter);
    }

    @Override
    public void processBroadcastElement(
            Rule rule, Context ctx, Collector<Keyed<SHCarRide, String, Long>> out) throws Exception {
        log.info("{}", rule);
        BroadcastState<Long, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);

        //????????????????????????????????????????????????????????????
        if (rule.getQueryState() != Rule.RuleState.DELETE && rule.getLastTime() > 0) {
            //???????????????????????????????????????????????????????????????????????????????????????????????????kafkasender???
//            Long timeNow = System.currentTimeMillis();
//            rule.setActiveTime(timeNow + rule.getLastTime());
        }
        //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        for (Map.Entry<Long, Rule> entry : broadcastState.immutableEntries()) {
            Rule compareRule = entry.getValue();
            List<WindowFilterRules> filterRules = compareRule.getWindowFilterRules();
            if (rule.getWindowFilterRules().equals(filterRules) && rule.getActiveId().equals(compareRule.getActiveId())) {
                //??????????????????????????????????????????????????????,???????????????
                rule.setActiveTime(System.currentTimeMillis() + rule.getLastTime());
                rule.setQueryId(compareRule.getQueryId());
            }
        }
        //??????ACTIVE???DELETE??????????????????????????????????????????
        handleRuleBroadcast(rule, broadcastState);

//        //*********************
//        ctx.output(
//                Descriptors.demoSinkTag,
//                "Rule "
//                        + rule.getQueryId()
//                        + " | "
//        );
//        //*********************

        if (rule.getQueryState() == Rule.RuleState.CONTROL) {
            handleControlCommand(rule.getControlType(), broadcastState);
        }
    }

    private void handleControlCommand(
            ControlType controlType, BroadcastState<Long, Rule> rulesState) throws Exception {
        switch (controlType) {
            case DELETE_RULES_ALL:
                Iterator<Map.Entry<Long, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Map.Entry<Long, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey());
                    log.info("Removed Rule {}", ruleEntry.getValue());
                }
                break;
        }
    }

    public boolean applyCompare(BigDecimal comparisonValue, Rule.LimitOperatorType operatorType, BigDecimal sourceValue) {
        switch (operatorType) {
            case EQUAL:
                return comparisonValue.compareTo(sourceValue) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(sourceValue) != 0;
            case GREATER:
                return comparisonValue.compareTo(sourceValue) > 0;
            case LESS:
                return comparisonValue.compareTo(sourceValue) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(sourceValue) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(sourceValue) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + operatorType);
        }
    }

    private static class RuleCounterGauge implements Gauge<Integer> {

        private int value = 0;

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public Integer getValue() {
            return value;
        }
    }


    public static class Descriptors {
        public static final MapStateDescriptor<Long, Rule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(Rule.class));

        public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {
        };
        public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
        };
        public static final OutputTag<Rule> currentRulesSinkTag =
                new OutputTag<Rule>("current-rules-sink") {
                };
    }
}

