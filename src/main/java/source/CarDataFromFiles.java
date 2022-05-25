package source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * @auther: zk
 * @date: 2022/3/24 15:14
 */
public class CarDataFromFiles extends RichSourceFunction<String> {
    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {

//        BufferedReader bufferedReader = new BufferedReader(new FileReader("src\\main\\resources\\1608011300.txt"));
//        String dir = "D:\\数据\\上海出租车\\01";
        String dir = "src\\main\\resources\\SHCarData";
        File[] filesNames = new File(dir).listFiles();
        int count = 0;
//        while (isRunning) {
        for (File filesName : filesNames) {
//            if (filesName.isDirectory()) {
//                String fileStr = filesName.getPath();
//                File[] txtNames = new File(fileStr).listFiles();
//                for (File file : txtNames) {
                    if (filesName.isFile() && filesName.exists()) {
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(filesName));
                        String line = null;
                        while ((line = bufferedReader.readLine()) != null) {
                            if (StringUtils.isBlank(line)) {
                                continue;
                            }
                            StringBuilder transcation = new StringBuilder();
                            //原始数据：00270|A|0|1|1|0|0|0|2016-08-01 13:00:00|2016-08-01 13:01:13|121.061075|31.401332|0.0|217.0|6|000
                            String[] values = line.split("\\|");
                            //车辆ID
                            transcation.append(values[0] + ",");
                            //事件时间,清洗2010-01-01的脏数据
                            transcation.append(values[9] + ",");
                            if (values[9].equals("2010-01-01") || values[9].equals("2016-08-01") || values[9].equals("2016-08-02")) {
                                continue;
                            }
                            //处理时间
                            transcation.append(Instant.now() + ",");
                            //经度
                            transcation.append(values[10] + ",");
                            //纬度
                            transcation.append(values[11] + ",");
                            //速度
                            transcation.append(values[12] + ",");
                            //角度
                            transcation.append(values[13]);

                            ctx.collect(transcation.toString());
                            count++;
                            if (count == 13000) {
                                //每秒读取13000条数据，同一车辆的两条数据之间相差12157条数据，通过这种方式模拟加速十倍的真实时间
                                //线程停止时间近似为车辆数据上报频率
                                TimeUnit.SECONDS.sleep(1);
                                count = 0;
                            }
                        }
                        bufferedReader.close();
                    }

                }
//            }
//        }

//        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}
