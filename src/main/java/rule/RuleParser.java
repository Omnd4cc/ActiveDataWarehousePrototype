package rule;

/**
 * @auther: zk
 * @date: 2021/12/20 16:55
 */

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RuleParser {

    private final ObjectMapper objectMapper = new ObjectMapper();


    public Rule fromString(String line) throws IOException {
        if (line.length() > 0 && '{' == line.charAt(0)) {
            return parseJson(line);
        } else {
            return parsePlain(line);
        }
    }

    private Rule parseJson(String ruleString) throws IOException {
        //	// 转换为格式化的json
//        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        // 如果json中有新增的字段并且是实体类类中不存在的，不报错
//        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(ruleString, Rule.class);
    }

    private static Rule parsePlain(String ruleString) throws IOException {
        List<String> tokens = Arrays.asList(ruleString.split(","));
//        if (tokens.size() != 9) {
//            throw new IOException("Invalid rule (wrong number of tokens): " + ruleString);
//        }

        Iterator<String> iter = tokens.iterator();
        Rule rule = new Rule();

//        rule.setQueryId(Integer.parseInt(stripBrackets(iter.next())));
//        rule.setQueryState(RuleState.valueOf(stripBrackets(iter.next()).toUpperCase()));
//        rule.setGroupingKeyNames(getNames(iter.next()));
//
//        rule.setAggregateFieldName(stripBrackets(iter.next()));
//        rule.setAggregatorFunctionType(
//                AggregatorFunctionType.valueOf(stripBrackets(iter.next()).toUpperCase()));
//        rule.setLimitOperatorType(LimitOperatorType.fromString(stripBrackets(iter.next())));
//        rule.setLimit(new BigDecimal(stripBrackets(iter.next())));

        return rule;
    }

    private static String stripBrackets(String expression) {
        return expression.replaceAll("[()]", "");
    }

    private static List<String> getNames(String expression) {
        String keyNamesString = expression.replaceAll("[()]", "");
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split("&", -1);
            return Arrays.asList(tokens);
        } else {
            return new ArrayList<>();
        }
    }
}
