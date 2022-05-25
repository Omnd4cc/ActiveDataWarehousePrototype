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
            //在处理过程中如果发现该规则已经超时了，那么使用throttle发送DELETE控制规则
            if (rule.getActiveTime() != null && rule.getActiveTime() < System.currentTimeMillis() && rule.getLastTime() > 0) {
                rule.setQueryState(Rule.RuleState.DELETE);
                KafkaSender sender = KafkaSender.getInstance();
                sender.sendRule(rule, null);
            }

            //窗口过滤规则生效
            for (WindowFilterRules filterRule : filterRules) {
                //使用Rule POJO类里的 apply函数思想，统一转化为BigDecimal进行比较

                //反射获取流数据里面对应的字段值
                Field field = event.getClass().getDeclaredField(filterRule.getField());
                //如果是字符串类型才可能有=的判断，所以通过对=进行特殊处理，而非对String进行处理。
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

        //如果是正常查询规则，计算其持续时间写入。
        if (rule.getQueryState() != Rule.RuleState.DELETE && rule.getLastTime() > 0) {
            //正常应该是这里进行激活时间的注册，为了完成主动查询生效时间，移动到kafkasender里
//            Long timeNow = System.currentTimeMillis();
//            rule.setActiveTime(timeNow + rule.getLastTime());
        }
        //判断是否是已触发查询，就是说这个查询已经被触发过再一次被新的数据触发了，需要更新激活时间。
        for (Map.Entry<Long, Rule> entry : broadcastState.immutableEntries()) {
            Rule compareRule = entry.getValue();
            List<WindowFilterRules> filterRules = compareRule.getWindowFilterRules();
            if (rule.getWindowFilterRules().equals(filterRules) && rule.getActiveId().equals(compareRule.getActiveId())) {
                //同一个主动规则被触发了，重设激活时间,更新该查询
                rule.setActiveTime(System.currentTimeMillis() + rule.getLastTime());
                rule.setQueryId(compareRule.getQueryId());
            }
        }
        //对于ACTIVE及DELETE规则都通过以下函数进行处理。
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

