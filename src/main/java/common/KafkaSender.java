package common;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import rule.Rule;


import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author 37327
 * @auther: zk
 * @date: 2022/1/6 12:42
 * @description: 使用单例模式，用于主动规则的发送，确保新的规则的id一致性
 */
@Slf4j
public class KafkaSender {
    private volatile static KafkaSender instance;
    private Integer ruleCount = 10;
    private static KafkaProducer<String, String> producer;
    private static String[] reqCache = new String[100];
    private static Integer reqCacheCounter = 0;


    private KafkaSender() {
        //连接到kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(properties);
    }

    public static KafkaSender getInstance() {
        if (instance == null) {
            synchronized (KafkaSender.class) {
                if (instance == null) {
                    instance = new KafkaSender();
                }
            }
        }
        return instance;
    }

    public void sendRule(Rule item, @Nullable Keyed<CarRide, String, Integer> value) {

        String id = String.valueOf(item.getQueryId());
        if (Arrays.asList(reqCache).contains(id)) {
            return;
        }
        synchronized (KafkaSender.class) {
            // 双重检查锁（double checked locking）提高程序的执行效率
            if (Arrays.asList(reqCache).contains(id)) {
                return;
            }
            // 记录请求 ID
            if (reqCacheCounter >= reqCache.length) {
                reqCacheCounter = 0;
            }
            reqCache[reqCacheCounter] = String.valueOf(id);
            reqCacheCounter++;
        }

        //如果是控制信息直接发送
        if (item.getQueryState() != Rule.RuleState.DELETE) {
            //将主动规则进行解析，主要是为了替换里面的字段变量
            //识别主动规则中的分组信息变量,可以使用Optional进行取值的优化
            List<String> keys = item.getGroupingKeyNames();
            List<String> fixedKeys = new ArrayList<String>();
            StringBuilder tmpKey = new StringBuilder();
            List<Rule.windowFilterRules> filterRules = item.getWindowFilterRules();
            for (String key : keys) {
                if (key.charAt(0) == '$') {
                    try {
                        tmpKey = tmpKey.append(key.substring(1, key.length()));
                        Field field = value.getWrapped().getClass().getField(tmpKey.toString());
                        String keyValue = String.valueOf(field.get(value.getWrapped()));
                        log.info(field + " = " + keyValue);
                        filterRules.add(new Rule.windowFilterRules(tmpKey.toString(), "=", keyValue));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else {
                    tmpKey = tmpKey.append(key);
                }
                fixedKeys.add(tmpKey.toString());
            }
            //重新赋值
            item.setWindowFilterRules(filterRules);
            item.setGroupingKeyNames(fixedKeys);
            item.setQueryId(ruleCount++);
        }


        ProducerRecord<String, String> record = new ProducerRecord<String, String>("rules", JSONObject.toJSONString(item));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
