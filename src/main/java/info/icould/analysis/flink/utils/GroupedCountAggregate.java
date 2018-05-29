package info.icould.analysis.flink.utils;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class GroupedCountAggregate implements AggregateFunction<Tuple3<String, String, String>, Tuple3<String,
        Map<String, Long>, String>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedCountAggregate.class);

    @Override
    public Tuple3<String, Map<String, Long>, String> createAccumulator() {
        return new Tuple3<>("New Tuple", new HashMap<>(), "hostname");
    }

    @Override
    public Tuple3<String, Map<String, Long>, String> add(Tuple3<String, String, String> value,  Tuple3<String,
            Map<String, Long>, String> accumulator) {
        // keep timestamp as it is the same (time window)
        accumulator.f0 = value.f0;
        // Http method key exists?
        if(accumulator.f1.containsKey(value.f1))
            accumulator.f1.replace(value.f1,accumulator.f1.get(value.f1) + 1);
        else
            accumulator.f1.put(value.f1, 1L);
        // keep the hostname, only one supported
        accumulator.f2 = value.f2;
        return new Tuple3<>(accumulator.f0, accumulator.f1,accumulator.f2);
    }

    @Override
    public String getResult(Tuple3<String, Map<String,Long>, String> accumulator) {
        StringBuilder sb = new StringBuilder();
        for(String key:accumulator.f1.keySet()){
            sb.append(format(";%s:00,%s,%d,%s;",accumulator.f0,key, accumulator.f1.get(key),accumulator.f2));
        }
        String result = sb.toString();
        if (LOG.isInfoEnabled()) {
            LOG.info("Aggregate result: " + result);
        }
        return result;
    }

    @Override
    public Tuple3<String, Map<String, Long>, String> merge(Tuple3<String, Map<String, Long>, String> leftTuple,
                                                           Tuple3<String, Map<String, Long>, String> rightTuple) {
        for(String rightKey:rightTuple.f1.keySet())
            if(leftTuple.f1.containsKey(rightKey)){
                leftTuple.f1.replace(rightKey,leftTuple.f1.get(rightKey) + rightTuple.f1.get(rightKey));
            } else {
                leftTuple.f1.put(rightKey, rightTuple.f1.get(rightKey));
            }

        return new Tuple3<>(leftTuple.f0 , leftTuple.f1, leftTuple.f2);
    }
}
