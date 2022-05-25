package rule;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * @auther: zk
 * @date: 2021/12/21 09:58
 * 另一种env.readFile，flink的官方接口也可以实现，没差。
 */
public class RuleFromFile extends RichSourceFunction {
    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("src\\main\\java\\sources\\rule.json"));
        while (isRunning) {
            String line = bufferedReader.readLine();
            if (StringUtils.isBlank(line)) {
                continue;
            }
//            JSONObject jobj = JSON.parseObject(line);
//            JSONArray rules = jobj.getJSONArray("rules");
//
//            for (int i= 0 ; i<rules.size();i++){
//                JSONObject key1 = (JSONObject)rules.get(i);
//                ctx.collect(key1.toString());
//                System.out.println(key1.toString());
//            }
            ctx.collect(line);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
