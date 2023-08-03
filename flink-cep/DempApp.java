import java.time.Duration;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class DempApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        //source
        DataStream<Tuple3<String, Long, String>> source = env.fromElements(
                new Tuple3<String, Long, String>("1001", 1656914303000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914304000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914305000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914306000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914307000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914308000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914309000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914310000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914311000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914312000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914313000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914314000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914315000L, "success")
                , new Tuple3<String, Long, String>("1001", 1656914316000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914317000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914318000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914319000L, "fail")
                , new Tuple3<String, Long, String>("1001", 1656914317000L, "end")
        ).assignTimestampsAndWatermarks(WatermarkStrategy

                .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) ->{
                    return event.f1;
                }));

        PatternStream patternStream = CEP.injectionPattern(source,new TestDynamicPatternFunction());

        patternStream.select(new PatternSelectFunction() {
            @Override
            public Object select(Map pattern) throws Exception {
                return pattern;
            }
        }).print();
        env.execute("SyCep");

    }

    public static class TestDynamicPatternFunction implements DynamicPatternFunction<Tuple3<String, Long, String>> {

        public TestDynamicPatternFunction() {
            this.flag = true;
        }

        boolean flag;
        @Override
        public void init() throws Exception {
            flag = true;
        }

        @Override
        public Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> inject()
                            throws Exception {

            // 随机2种pattern
            // 1.success 必须连续匹配2次，接下来是fail
            // 2.success 连续匹配3次，接下来fail
            if (flag) {

                return Pattern
                        .<Tuple3<String, Long, String>>begin("start")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success");
                            }
                        })
                        .times(2)
                        .followedByAny("middle")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail");
                            }
                        })
                        .times(1)
                        .followedBy("end");
            } else {
                return Pattern
                        .<Tuple3<String, Long, String>>begin("start")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("success");
                            }
                        })
                        .times(3)
                        .followedByAny("middle")
                        .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, String> value,
                                    Context<Tuple3<String, Long, String>> ctx) throws Exception {
                                return value.f2.equals("fail");
                            }
                        })
                        .times(1)
                        .followedBy("end");
            }
        }

        @Override
        public long getPeriod() throws Exception {
            return 1000;
        }

        @Override
        public boolean isChanged() throws Exception {
            //确保实验中只变换一次规则，以便观察实验结果
            if ((System.currentTimeMillis() / 1000) % 5 == 0 && flag) {
                flag = false;
                return true;
            }
            return false;
        }
    }
}
