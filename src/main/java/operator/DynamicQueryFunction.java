package operator;

import common.Alert;
import common.CarRide;
import common.KafkaSender;
import common.Keyed;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static common.ProcessingUtils.handleRuleBroadcast;
import static common.utils.ProcessUtils.addToStateValuesSet;

/**
 * @auther: zk
 * @date: 2021/12/24 10:54
 */
@Slf4j
public class DynamicQueryFunction extends KeyedBroadcastProcessFunction<
        String, Keyed<CarRide, String, Integer>, Rule, Alert> {

    private static final String COUNT = "COUNT_FLINK";
    private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

    private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
    private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

    private transient MapState<Long, Set<CarRide>> windowState;
    private Meter alertMeter;

    private MapStateDescriptor<Long, Set<CarRide>> windowStateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Set<CarRide>>() {
                    }));

    @Override
    public void open(Configuration parameters) {

        windowState = getRuntimeContext().getMapState(windowStateDescriptor);

        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    }

    @Override
    public void processElement(
            Keyed<CarRide, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
            throws Exception {

        long currentEventTime = value.getWrapped().getEventTimeMillis();

        addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

        long ingestionTime = value.getWrapped().getEventTimeMillis();
        ctx.output(DynamicKeyFunction.Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

        Rule rule = ctx.getBroadcastState(DynamicKeyFunction.Descriptors.rulesDescriptor).get(value.getId());

        if (noRuleAvailable(rule)) {
            log.error("Rule with ID {} does not exist", value.getId());
            return;
        }

        if (rule.getQueryState() == Rule.RuleState.ACTIVE) {
            Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

            long cleanupTime = (currentEventTime / 1000) * 1000;
            ctx.timerService().registerEventTimeTimer(cleanupTime);

            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowState.keys()) {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
                    aggregateValuesInState(stateEventTime, aggregator, rule);
                }
            }
            BigDecimal aggregateResult = aggregator.getLocalValue();
            boolean ruleResult = rule.apply(aggregateResult);

            ctx.output(
                    DynamicKeyFunction.Descriptors.demoSinkTag,
                    "Rule "
                            + rule.getQueryId()
                            + " | "
                            + value.getKey()
                            + " : "
                            + aggregateResult.toString()
                            + " -> "
                            + ruleResult);

            if (ruleResult && rule.getAlertRules() != null) {
                KafkaSender sender = KafkaSender.getInstance();
                for (Rule item : rule.getAlertRules()) {
                    sender.sendRule(item, value);
                }


//                if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
//                    evictAllStateElements();
//                }
                alertMeter.markEvent();
                out.collect(
                        new Alert<>(
                                rule.getQueryId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
            }
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
            throws Exception {
        log.info("{}", rule);
        BroadcastState<Integer, Rule> broadcastState =
                ctx.getBroadcastState(DynamicKeyFunction.Descriptors.rulesDescriptor);
        handleRuleBroadcast(rule, broadcastState);
        if (rule.getQueryState() != Rule.RuleState.DELETE) {
            updateWidestWindowRule(rule, broadcastState);
        }

        if (rule.getQueryState() == Rule.RuleState.CONTROL) {
            handleControlCommand(rule, broadcastState, ctx);
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
        Set<CarRide> inWindow = windowState.get(stateEventTime);
        if (COUNT.equals(rule.getAggregateFieldName())
                || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
            for (CarRide event : inWindow) {
                aggregator.add(BigDecimal.ONE);
            }
        } else {
            for (CarRide event : inWindow) {
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

    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
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
            Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
        Rule.ControlType controlType = command.getControlType();
        switch (controlType) {
            case EXPORT_RULES_CURRENT:
                for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
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
                Iterator<Map.Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Map.Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey());
                    log.info("Removed Rule {}", ruleEntry.getValue());
                }
                break;
        }
    }

}
