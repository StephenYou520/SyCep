import java.io.File;
import java.io.FileOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import cep.CEP;
import cep.PatternSelectFunction;
import cep.PatternStream;
import cep.functions.DynamicPatternFunction;
import cep.pattern.Pattern;
import cep.pattern.conditions.IterativeCondition;

/**
 * @author StephenYou
 * Created on 2023-07-22
 * Description:
 */
public class DynamicPatternApp {

    //测试数据
    private static final String filePath = "/Users/yqz/project/data.txt";

    public static void main(String[] args) throws Exception {
        writeToTxt();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        TextInputFormat format = new TextInputFormat(new Path(filePath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");

        //source
        DataStream<Tuple3<String, Long, String>> source = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10, typeInfo)
                .map(new MapFunction<String, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> map(String s) throws Exception {
                        String[] split = s.split(" ");
                        Tuple3 res = new Tuple3<>(split[0], Long.valueOf(split[1]), split[2]);
                        return res;
                    }
                })
        .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) ->{
                    return event.f1;
                }));

        PatternStream patternStream = CEP.injectionPattern(source, new TestDynamicPatternFunction());
        patternStream.select(new PatternSelectFunction<Tuple3<String, Long, String>, Map>() {
            @Override
            public Map select(Map map) throws Exception {
                map.put("processingTime", System.currentTimeMillis());
                return map;
            }
        }).print();
        env.execute("SyCep");

    }

    public static class TestDynamicPatternFunction implements DynamicPatternFunction<Tuple3<String, Long, String>> {

        public TestDynamicPatternFunction() {
            this.flag = true;
        }

        boolean flag;
        int time = 0;
        @Override
        public void init() throws Exception {
            flag = true;
        }

        @Override
        public Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> inject()
                            throws Exception {

            // 2种pattern
            if (flag) {
                Pattern pattern = Pattern
                        .<Tuple3<String, Long, String>>begin("start")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success");
                            }
                        })
                        .times(1)
                        .followedBy("middle")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail");
                            }
                        })
                        .times(1)
                        .next("end");
                return pattern;
            } else {

                Pattern pattern = Pattern
                        .<Tuple3<String, Long, String>>begin("start2")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success2");
                            }
                        })
                        .times(2)
                        .next("middle2")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail2");
                            }
                        })
                        .times(2)
                        .next("end2");
                return pattern;
            }
        }

        @Override
        public long getPeriod() throws Exception {
            return 10000;
        }

        @Override
        public boolean isChanged() throws Exception {
            flag = !flag ;
            time += getPeriod();
            System.out.println("change pattern : " + time);
            return true;
        }
    }

    private static void writeToTxt() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileOutputStream stream = new FileOutputStream(new File(filePath));
                    while (true) {

                        StringBuilder builder = new StringBuilder();
                        if (System.currentTimeMillis() % 3 == 0) {
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis());
                            builder.append(" success\n");
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis() + 1);
                            builder.append(" fail\n");
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis() + 2);
                            builder.append(" end\n");
                        } else {
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis());
                            builder.append(" success2\n");
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis() + 1);
                            builder.append(" fail2\n");
                            builder.append("1001 ");
                            builder.append(System.currentTimeMillis() + 2);
                            builder.append(" end2\n");
                        }
                        stream.write(builder.toString().getBytes());
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {

                    throw new RuntimeException(e);
                }
            }
        }));
    }
}
