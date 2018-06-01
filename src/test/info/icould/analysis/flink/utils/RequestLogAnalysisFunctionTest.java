package info.icould.analysis.flink.utils;

import info.icould.analysis.flink.domain.IncomingLine;
import info.icould.analysis.flink.domain.OutgoingLine;
import info.icould.analysis.flink.domain.RequestEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestLogAnalysisFunctionTest {

  @Test
  void logLineMapperShouldReturnDateMethodAndHost() {
    LogLineMapper logLineMapper = new LogLineMapper(" ", 3, 4, 7, 8);
    assertEquals(new Tuple4<>("2018-03-06T18:14", "GET", "bumpkin", "->"),
            logLineMapper.map("127.0.0.1:<14>Mar  6 18:13:13 bumpkin 06/Mar/2018:18:13:13 +0100 [50] -> GET /libs/cq/gui/components/coral/common/admin/timeline/alerts/workflow/clientlibs/workflow.js HTTP/1.1"));
  }

  @Test
  void logLineMapperShouldReturnDateMethodAndHost2() {
    LogLineMapper logLineMapper = new LogLineMapper(" ", 3, 4, 7, 8);
    assertEquals(new Tuple4<>("2018-03-12T11:27", "GET", "bumpkin", "->"),
            logLineMapper.map("127.0.0.1:<14>Mar 12 11:26:54 bumpkin 12/Mar/2018:11:26:54 +0100 [29] -> GET /mnt/overlay/granite/ui/content/shell/start/content/cq/items/tabs/items/home/content.0.20.html/mnt/overlay/cq/core/content/nav/assets?_=1520850311517 HTTP/1.1"));
  }

  @Test
  void groupedCountAggregateShouldReturnCorrectResult() {
    GroupedCountAggregate groupedCountAggregate = new GroupedCountAggregate();
    Tuple3<String, Map<String, Long>, String> accumulator = groupedCountAggregate.createAccumulator();
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), accumulator);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), accumulator);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), accumulator);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), accumulator);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "POST", "bumpkin"), accumulator);
    assertEquals(";2018-01-23 15:47:00,POST,1,bumpkin;;2018-01-23 15:47:00,GET,4,bumpkin;",
            groupedCountAggregate.getResult(accumulator));
  }

  @Test
  void groupedCountAggregateShouldMergeTuplesCorrectly() {
    GroupedCountAggregate groupedCountAggregate = new GroupedCountAggregate();
    Tuple3<String, Map<String, Long>, String> leftTuple = groupedCountAggregate.createAccumulator();
    Tuple3<String, Map<String, Long>, String> rightTuple = groupedCountAggregate.createAccumulator();
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), leftTuple);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), leftTuple);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), rightTuple);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "GET", "bumpkin"), rightTuple);
    groupedCountAggregate.add(new Tuple3<>("2018-01-23 15:47", "POST", "bumpkin"), rightTuple);
    Tuple3<String, Map<String, Long>, String> accumulator = groupedCountAggregate.merge(leftTuple, rightTuple);
    assertEquals(";2018-01-23 15:47:00,POST,1,bumpkin;;2018-01-23 15:47:00,GET,4,bumpkin;",
            groupedCountAggregate.getResult(accumulator));
  }

  @Test
  void joinRequestFunctionShouldReturnRequestEvent() {
    JoinRequestFunction joinRequestFunction = new JoinRequestFunction();
    IncomingLine incomingLine = new IncomingLine("192.168.50.1", "01/Jun/2018:16:14:59", 178L,
            "GET", "/content/dam/test1/004578661466.png.925x925_q90.jpg/_jcr_content/metadata.json", "HTTP/1.1");
    OutgoingLine outgoingLine = new OutgoingLine("192.168.50.1", "01/Jun/2018:16:14:59", 178L,
            "200", "application/json", 3L);
    RequestEvent requestEvent = new RequestEvent(incomingLine, outgoingLine);
    assertEquals(requestEvent.toJson(), joinRequestFunction.join(incomingLine, outgoingLine).toJson());

  }

  @Test
  void incomingLineMapperShouldBuildCorrectObject() {
    IncomingLineMapper incomingLineMapper = new IncomingLineMapper();
    IncomingLine incomingLine = new IncomingLine("192.168.50.1", "2018-06-01T16:14:59", 178L,
            "GET", "/content/dam/test1/004578661466.png.925x925_q90.jpg/_jcr_content/metadata.json", "HTTP/1.1");
    try {
      assertEquals(incomingLine.toJson(),
              incomingLineMapper.map("2018-06-01T16:14:59.000Z 192.168.50.1 01/Jun/2018:16:14:59 +0200 [178] -> GET /content/dam/test1/004578661466.png.925x925_q90.jpg/_jcr_content/metadata.json HTTP/1.1").toJson());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void outgoingLineMapperShouldBuildCorrectObject() {
    OutgoingLineMapper outgoingLineMapper = new OutgoingLineMapper();
    OutgoingLine outgoingLine = new OutgoingLine("192.168.50.1", "2018-06-01T16:14:59", 178L,
            "200", "application/json", 3L);
    try {
      assertEquals(outgoingLine.toJson(),
              outgoingLineMapper.map("2018-06-01T16:14:59.000Z 192.168.50.1 01/Jun/2018:16:14:59 +0200 [178] <- 200 application/json 3ms").toJson());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
