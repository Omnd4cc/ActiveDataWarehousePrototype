package rule;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @auther: zk
 * @date: 2021/12/21 19:13
 */
@Slf4j
public class RuleDeserializer extends RichFlatMapFunction<String, Rule> {
    private RuleParser ruleParser;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ruleParser = new RuleParser();
    }

    @Override
    public void flatMap(String value, Collector<Rule> out) throws Exception {
        log.info("{}", value);
        try {
            if (value.length() > 0) {
                Rule rule = ruleParser.fromString(value);
                if (rule.getQueryState() != Rule.RuleState.CONTROL && rule.getQueryId() == null) {
                    throw new NullPointerException("ruleId cannot be null: " + rule.toString());
                }
                out.collect(rule);
            } else {
//                    throw new NullPointerException("rule cannot be null");
            }


        } catch (Exception e) {
            log.warn("Failed parsing rule, dropping it:", e);
        }
    }

}
