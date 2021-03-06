package operator;

import common.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import rule.Rule;

import rule.RuleHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static common.ProcessingUtils.handleRuleBroadcast;
import static common.utils.ProcessUtils.addToStateValuesSet;

/**
 * @auther: zk
 * @date: 2021/12/24 10:54
 */
@Slf4j
public class DynamicQueryFunction extends KeyedBroadcastProcessFunction<
        String, Keyed<SHCarRide, String, Long>, Rule, Alert> {

    private static final String COUNT = "COUNT_FLINK";
    private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

    private static long WIDEST_RULE_KEY = Long.MIN_VALUE;
    private static long CLEAR_STATE_COMMAND_KEY = Long.MIN_VALUE + 1;

    private transient MapState<Long, Set<SHCarRide>> windowState;
    private transient ValueState<Boolean> outputState;
    private Meter alertMeter;

    private MapStateDescriptor<Long, Set<SHCarRide>> windowStateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Set<SHCarRide>>() {
                    }));

    private ValueStateDescriptor<Boolean> outPutFlagDescriptor =
            new ValueStateDescriptor<>(
                    "outputState",
                    BasicTypeInfo.BOOLEAN_TYPE_INFO);


    @Override
    public void open(Configuration parameters) throws IOException {

        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
        outputState = getRuntimeContext().getState(outPutFlagDescriptor);

        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    }

    @Override
    public void processElement(
            Keyed<SHCarRide, String, Long> value, ReadOnlyContext ctx, Collector<Alert> out)
            throws Exception {
        long currentEventTime = value.getWrapped().getProcessTimeMillis();
        addToStateValuesSet(windowState, currentEventTime, value.getWrapped());
        //?????????
        if (outputState.value() == null) {
            outputState.update(false);
        }

        //????????????????????????????????????
        ctx.output(DynamicKeyFunction.Descriptors.latencySinkTag, System.currentTimeMillis() - currentEventTime);

        Rule rule = ctx.getBroadcastState(DynamicKeyFunction.Descriptors.rulesDescriptor).get(value.getId());

        if (noRuleAvailable(rule)) {
            log.error("Rule with ID {} does not exist", value.getId());
            return;
        }

        //??????rule???????????????,??????????????????????????????????????????
        if (rule.getQueryState() == Rule.RuleState.ACTIVE) {
            //?????????????????????0,???????????????????????????
            if (rule.getWindowMilliseconds() <= 0) {
                ctx.output(
                        DynamicKeyFunction.Descriptors.demoSinkTag,
                        "Rule "
                                + rule.getQueryId()
                                + ","
                                + value.getKey()
                                + ","
                                + value.getWrapped().getEventTime()
                                + ","
                                + value.getWrapped().getSpeed()
                                + ","
                                + value.getWrapped().getLat()
                                + ","
                                + value.getWrapped().getLon()
                                + ","
                                + String.valueOf(Duration.between(value.getWrapped().getProcessTime(), Instant.now()))
                                + ","
                                + 0
                                + ","
                                + false
                );
                return;
            }
            //???????????????0???????????????????????????????????????,???????????????null??????????????????????????????????????????????????????????????????
            if (rule.getFrequencyMilliseconds() == 0) {
                agggregateEvent(currentEventTime, rule, value, out, ctx);
                return;
            }
            //????????????????????????????????????????????????????????????????????????????????????????????????
            if (rule.getFrequencyMilliseconds() == null || rule.getFrequencyMilliseconds() > rule.getWindowMilliseconds()) {
                //?????????????????????????????????????????????????????????????????????????????????????????????false?????????????????????????????????????????????
                if (outputState.value()) {
                    agggregateEvent(currentEventTime, rule, value, out, ctx);
                    outputState.update(false);
                }
                //????????????????????????????????????????????????
                double doubleActiveTime = Math.floor(System.currentTimeMillis() / rule.getWindowMilliseconds())
                        * rule.getWindowMilliseconds() + rule.getWindowMilliseconds();
                long activeTime = new Double(doubleActiveTime).longValue();
                ctx.timerService().registerProcessingTimeTimer(activeTime);
                return;
            }

            //????????????????????????????????????,??????outputState
            if (outputState.value()) {
                agggregateEvent(currentEventTime, rule, value, out, ctx);
                outputState.update(false);
            }
            //????????????????????????????????????????????????
            double doubleActiveTime = Math.floor(System.currentTimeMillis() / rule.getFrequencyMilliseconds())
                    * rule.getFrequencyMilliseconds() + rule.getFrequencyMilliseconds();
            long activeTime = new Double(doubleActiveTime).longValue();
            ctx.timerService().registerProcessingTimeTimer(activeTime);
        }
    }

    private void agggregateEvent(Long currentEventTime,
                                 Rule rule, Keyed<SHCarRide, String, Long> value,
                                 Collector<Alert> out,
                                 ReadOnlyContext ctx) throws Exception {
        Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

        long cleanupTime = (currentEventTime / 1000) * 1000;
        ctx.timerService().registerProcessingTimeTimer(cleanupTime);

        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
        for (Long stateEventTime : windowState.keys()) {
            if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
                aggregateValuesInState(stateEventTime, aggregator, rule);
            }
        }
        BigDecimal aggregateResult = aggregator.getLocalValue();
        boolean ruleResult = rule.apply(aggregateResult);

        String delay = String.valueOf(Duration.between(value.getWrapped().getProcessTime(), Instant.now()));
//            ctx.output(DynamicKeyFunction.Descriptors.latencySinkTag,Long.valueOf(delay));
        ctx.output(
                DynamicKeyFunction.Descriptors.demoSinkTag,
                "Rule "
                        + rule.getQueryId()
                        + ","
                        + value.getKey()
                        + ","
                        + value.getWrapped().getEventTime()
                        + ","
                        + value.getWrapped().getSpeed()
                        + ","
                        + value.getWrapped().getLat()
                        + ","
                        + value.getWrapped().getLon()
                        + ","
                        + delay
                        + ","
                        + aggregateResult
                        + ","
                        + ruleResult
        );


        if (ruleResult && rule.getAlertRules() != null) {
            KafkaSender sender = KafkaSender.getInstance();
            for (Rule item : rule.getAlertRules()) {
                sender.sendRule(item, value);
            }

            alertMeter.markEvent();
            out.collect(
                    new Alert<>(
                            rule.getQueryId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
            throws Exception {
        log.info("{}", rule);
        BroadcastState<Long, Rule> broadcastState =
                ctx.getBroadcastState(DynamicKeyFunction.Descriptors.rulesDescriptor);


        //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        for (Map.Entry<Long, Rule> entry : broadcastState.immutableEntries()) {
            Rule compareRule = entry.getValue();
            List<WindowFilterRules> filterRules = compareRule.getWindowFilterRules();
            if (rule.getWindowFilterRules().equals(filterRules) && rule.getActiveId().equals(compareRule.getActiveId())) {
                //??????????????????????????????????????????????????????,????????????????????????????????????dynamicKeyfunction???????????????????????????
//                rule.setActiveTime(System.currentTimeMillis() + rule.getLastTime());
                rule.setQueryId(compareRule.getQueryId());
            }
        }
        handleRuleBroadcast(rule, broadcastState);
//        System.out.println("lengency"+(System.currentTimeMillis()-rule.getActiveTime()));

        if (rule.getQueryState() != Rule.RuleState.DELETE) {
            updateWidestWindowRule(rule, broadcastState);
        }

        if (rule.getQueryState() == Rule.RuleState.CONTROL) {
            handleControlCommand(rule, broadcastState, ctx);
        }
    }

    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
            throws Exception {
        //????????????????????????????????????????????????
        outputState.update(true);


        //???????????????????????????????????????????????????????????????????????????????????????????????????
        Rule widestWindowRule = ctx.getBroadcastState(DynamicKeyFunction.Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

        Optional<Long> cleanupEventTimeWindow =
                Optional.ofNullable(widestWindowRule).map(Rule::getWindowMilliseconds);
        Optional<Long> cleanupEventTimeThreshold =
                cleanupEventTimeWindow.map(window -> timestamp - window);

        cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
    }

    private void evictAgedElementsFromWindow(Long threshold) {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                Long stateEventTime = keys.next();
                if (stateEventTime < threshold) {
                    keys.remove();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean noRuleAvailable(Rule rule) {
        // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
        // updated and used in `DynamicKeyFunction`
        if (rule == null) {
            return true;
        }
        return false;
    }

    private boolean isStateValueInWindow(
            Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
        return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
    }

    private void aggregateValuesInState(
            Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
        Set<SHCarRide> inWindow = windowState.get(stateEventTime);
        if (COUNT.equals(rule.getAggregateFieldName())
                || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
            for (SHCarRide event : inWindow) {
                aggregator.add(BigDecimal.ONE);
            }
        } else {
            for (SHCarRide event : inWindow) {
                BigDecimal aggregatedValue =
                        FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
                aggregator.add(aggregatedValue);
            }
        }
    }

    private void evictAllStateElements() {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                keys.next();
                keys.remove();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void updateWidestWindowRule(Rule rule, BroadcastState<Long, Rule> broadcastState)
            throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

        if (rule.getQueryState() != Rule.RuleState.ACTIVE) {
            return;
        }

        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
            return;
        }

        if (widestWindowRule.getWindowMilliseconds() < rule.getWindowMilliseconds()) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
        }
    }

    private void handleControlCommand(
            Rule command, BroadcastState<Long, Rule> rulesState, Context ctx) throws Exception {
        Rule.ControlType controlType = command.getControlType();
        switch (controlType) {
            case EXPORT_RULES_CURRENT:
                for (Map.Entry<Long, Rule> entry : rulesState.entries()) {
                    ctx.output(DynamicKeyFunction.Descriptors.currentRulesSinkTag, entry.getValue());
                }
                break;
            case CLEAR_STATE_ALL:
                ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
                break;
            case CLEAR_STATE_ALL_STOP:
                rulesState.remove(CLEAR_STATE_COMMAND_KEY);
                break;
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

}
