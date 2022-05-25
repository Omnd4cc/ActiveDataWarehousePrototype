package common;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import rule.Rule;


import java.util.HashSet;
import java.util.Set;


/**
 * @auther: zk
 * @date: 2021/12/22 15:50
 */
public class ProcessingUtils {

    public static void handleRuleBroadcast(Rule rule, BroadcastState<Long, Rule> broadcastState)
            throws Exception {
        switch (rule.getQueryState()) {
            case ACTIVE:
            case PAUSE:
                broadcastState.put(rule.getQueryId(), rule);
                break;
            case DELETE:
                broadcastState.remove(rule.getQueryId());
                break;
        }
    }

    static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
            throws Exception {

        Set<V> valuesSet = mapState.get(key);

        if (valuesSet != null) {
            valuesSet.add(value);
        } else {
            valuesSet = new HashSet<>();
            valuesSet.add(value);
        }
        mapState.put(key, valuesSet);
        return valuesSet;
    }
}
