package rule;

/**
 * @auther: zk
 * @date: 2021/12/20 16:53
 */

import common.accumulators.AverageAccumulator;
import common.accumulators.BigDecimalCounter;
import common.accumulators.BigDecimalMaximum;
import common.accumulators.BigDecimalMinimum;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/* Collection of helper methods for Rules. */
public class RuleHelper {

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregatorFunctionType()) {
            case SUM:
                return new BigDecimalCounter();
            case AVG:
                return new AverageAccumulator();
            case MAX:
                return new BigDecimalMaximum();
            case MIN:
                return new BigDecimalMinimum();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
        }
    }
}
