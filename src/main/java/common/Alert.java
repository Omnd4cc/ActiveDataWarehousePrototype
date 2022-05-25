package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import rule.Rule;


/**
 * @auther: zk
 * @date: 2021/12/24 10:58
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Alert<Event, Value> {
    private Integer ruleId;
    private Rule violatedRule;
    private String key;

    private Event triggeringEvent;
    private Value triggeringValue;
}
