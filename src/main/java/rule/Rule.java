package rule;
import common.WindowFilterRules;
import lombok.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;


/**
 * @auther: zk
 * @date: 2021/12/9 16:54
 */
@EqualsAndHashCode
@ToString
@Data
public class Rule implements Serializable {

    private Long queryId;
    private RuleState queryState;
    private List<WindowFilterRules> windowFilterRules;
    private List<String> groupingKeyNames;
    private String aggregateFieldName;
    private AggregatorFunctionType aggregatorFunctionType;
    private LimitOperatorType limitOperatorType;
    private BigDecimal limit;
    private Long windowMilliseconds;
    private Long frequencyMilliseconds;
    private List<Rule> alertRules;
    private Long activeTime;
    private Long lastTime;
    private ControlType controlType;
    private Long activeId;

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("{");
        for(Field field : Rule.class.getDeclaredFields()){
            try {
                result
                        .append(field.getName())
                        .append(":")
                        .append(field.get(Rule.this))
                        .append(",");
            }catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return result.substring(0,result.length()-1) + "}";
    }
    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }


    public long getWindowStartFor(Long timestamp) {
        Long ruleWindowMillis = this.windowMilliseconds;
        return (timestamp - ruleWindowMillis);
    }

    public enum AggregatorFunctionType {
        SUM,
        AVG,
        MIN,
        MAX,
    }

    public enum LimitOperatorType {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        GREATER(">"),
        LESS("<");

        String operator;

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        public static LimitOperatorType fromString(String text) {
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    public enum RuleState {
        ACTIVE,
        PAUSE,
        DELETE,
        CONTROL
    }

    public enum ControlType {
        CLEAR_STATE_ALL,
        CLEAR_STATE_ALL_STOP,
        DELETE_RULES_ALL,
        EXPORT_RULES_CURRENT
    }


}
