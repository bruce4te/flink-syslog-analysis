package info.icould.analysis.flink.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestLogAnalysisFunctionTest {

    @Test
    void logLineMapperShouldReturnDateMethodAndHost() {
        LogLineMapper logLineMapper = new LogLineMapper(" ", 3, 4, 7, 8);
        assertEquals(new Tuple4<>("2018-03-06T18:14","GET","bumpkin","->") ,
                logLineMapper.map("127.0.0.1:<14>Mar  6 18:13:13 bumpkin 06/Mar/2018:18:13:13 +0100 [50] -> GET /libs/cq/gui/components/coral/common/admin/timeline/alerts/workflow/clientlibs/workflow.js HTTP/1.1"));
    }

    @Test
    void logLineMapperShouldReturnDateMethodAndHost2() {
        LogLineMapper logLineMapper = new LogLineMapper(" ", 3, 4, 7, 8);
        assertEquals(new Tuple4<>("2018-03-12T11:27","GET","bumpkin","->") ,
                logLineMapper.map("127.0.0.1:<14>Mar 12 11:26:54 bumpkin 12/Mar/2018:11:26:54 +0100 [29] -> GET /mnt/overlay/granite/ui/content/shell/start/content/cq/items/tabs/items/home/content.0.20.html/mnt/overlay/cq/core/content/nav/assets?_=1520850311517 HTTP/1.1"));
    }

    @Test
    void groupedCountAggregateShouldReturnCorrectResult() {
        GroupedCountAggregate groupedCountAggregate = new GroupedCountAggregate();
        Tuple3<String, Map<String, Long>, String> accumulator = groupedCountAggregate.createAccumulator();
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), accumulator);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), accumulator);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), accumulator);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), accumulator);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","POST","bumpkin"), accumulator);
        assertEquals(";2018-01-23 15:47:00,POST,1,bumpkin;;2018-01-23 15:47:00,GET,4,bumpkin;",
                groupedCountAggregate.getResult(accumulator));
    }

    @Test
    void groupedCountAggregateShouldMergeTuplesCorrectly() {
        GroupedCountAggregate groupedCountAggregate = new GroupedCountAggregate();
        Tuple3<String, Map<String, Long>, String> leftTuple = groupedCountAggregate.createAccumulator();
        Tuple3<String, Map<String, Long>, String> rightTuple = groupedCountAggregate.createAccumulator();
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), leftTuple);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), leftTuple);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), rightTuple);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","GET","bumpkin"), rightTuple);
        groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47","POST","bumpkin"), rightTuple);
        Tuple3<String, Map<String, Long>, String> accumulator = groupedCountAggregate.merge(leftTuple,rightTuple);
        assertEquals(";2018-01-23 15:47:00,POST,1,bumpkin;;2018-01-23 15:47:00,GET,4,bumpkin;",
                groupedCountAggregate.getResult(accumulator));
    }
}
