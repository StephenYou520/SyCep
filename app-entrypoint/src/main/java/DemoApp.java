import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cep.CEP;
import cep.PatternSelectFunction;
import cep.PatternStream;
import cep.pattern.Pattern;
import cep.pattern.conditions.IterativeCondition;

/**
 * @author StephenYou
 * Created on 2023-07-29
 * Description: run for show source code
 */
public class DemoApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);


        KeyedStream<Tuple3<String, Long, String>, String> source = env.fromElements(
                        new Tuple3<String, Long, String>("1001", 1656914303000L, "success")
                        , new Tuple3<String, Long, String>("1001", 1656914304000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914305000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914306000L, "success")
                        , new Tuple3<String, Long, String>("1001", 1656914307000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914308000L, "success")
                        , new Tuple3<String, Long, String>("1001", 1656914309000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914310000L, "success")
                        , new Tuple3<String, Long, String>("1001", 1656914311000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914312000L, "fail")
                        , new Tuple3<String, Long, String>("1001", 1656914313000L, "success")
                        , new Tuple3<String, Long, String>("1001", 1656914314000L, "end")
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) ->{
                            return event.f1;
                        }))

                .keyBy(e -> e.f0);


        Pattern<Tuple3<String, Long, String>,?> pattern = Pattern
                .<Tuple3<String, Long, String>>begin("begin")
                .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx)
                            throws Exception {
                        return value.f2.equals("success");
                    }
                })
                .followedByAny("middle")
                .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx)
                            throws Exception {
                        return value.f2.equals("fail");
                    }
                })
                .followedBy("end")
                .where(new IterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx)
                            throws Exception {
                        return value.f2.equals("end");
                    }
                });

        PatternStream patternStream = CEP.pattern(source, pattern);

        patternStream.select(new PatternSelectFunction<Tuple3<String, Long, String>, Map<String, List<Tuple3<String, Long, String>>>>() {

            @Override
            public Map<String, List<Tuple3<String, Long, String>>> select(Map<String, List<Tuple3<String, Long, String>>> pattern)
                    throws Exception {
                return pattern;
            }
        }).print();
        env.execute("cep");
    }

}
