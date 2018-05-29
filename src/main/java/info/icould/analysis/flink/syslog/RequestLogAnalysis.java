package info.icould.analysis.flink.syslog;

import info.icould.analysis.flink.domain.RequestEvent;
import info.icould.analysis.flink.utils.Constants;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class RequestLogAnalysis {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // make parameters available in the web interface
        see.getConfig().setGlobalJobParameters(params);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.getRequired(Constants.KAFKA_BOOTSTRAP_SERVERS));

        DataStream<String> allRequestLogs = see
                .addSource(new FlinkKafkaConsumer011<>(params.getRequired(Constants.KAFKA_TOPIC), new SimpleStringSchema(), properties));

        allRequestLogs.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {

            @Override
            public long extractAscendingTimestamp(String element) {
                String timestampStr = element.split(" ")[2];
                DateTimeFormatter accessFormatter = DateTimeFormatter.ofPattern(Constants.ACCESS_LOG_DF, Locale.ENGLISH);
                LocalDateTime logDate = LocalDateTime.parse(timestampStr, accessFormatter);
                Timestamp timestamp = Timestamp.valueOf(logDate);
                return timestamp.getTime();
            }
        });

        SplitStream<String> splitStream = allRequestLogs.split((OutputSelector<String>) s -> {
            List<String> output = new ArrayList<>();
            if(s.contains("->")){
                output.add("incoming");
            } else {
                output.add("outgoing");
            }
            return output;
        });
        DataStream<Tuple6<String,String,Long, String,String, String>> incomingStream = splitStream
                .select("incoming")
                .map(s -> {
                    String[] lineParts = s.split(" ");
                    DateTimeFormatter accessFormatter = DateTimeFormatter.ofPattern(Constants.ACCESS_LOG_DF, Locale.ENGLISH);
                    LocalDateTime logDate = LocalDateTime.parse(lineParts[2], accessFormatter);
                    String timestamp = logDate.toString();
                    long requestId = Long.parseLong(lineParts[4].replace("[","").replace("]", ""));
                    return new Tuple6<>(lineParts[1],timestamp,
                            requestId,lineParts[6],lineParts[7],lineParts[8]);
                });

        DataStream<Tuple6<String,String,Long,String,String,Long>> outgoingStream = splitStream
                .select("outgoing")
                .map(s -> {
                    String[] lineParts = s.split(" ");
                    DateTimeFormatter accessFormatter = DateTimeFormatter.ofPattern(Constants.ACCESS_LOG_DF, Locale.ENGLISH);
                    LocalDateTime logDate = LocalDateTime.parse(lineParts[2], accessFormatter);
                    String timestamp = logDate.toString();
                    long requestId = Long.parseLong(lineParts[4].replace("[","").replace("]", ""));
                    long duration = Long.parseLong(lineParts[lineParts.length-1].replace("ms",""));
                    return new Tuple6<>(lineParts[1],timestamp,
                            requestId,lineParts[6],lineParts[7],duration);
                });
        
        DataStream<RequestEvent> resultStream = incomingStream
                .join(outgoingStream)
                .where((KeySelector<Tuple6<String, String, Long, String, String, String>, Tuple2<Long,String>>) incoming -> new Tuple2<>(incoming.f2,incoming.f0))
                .equalTo((KeySelector<Tuple6<String, String, Long, String, String, Long>, Tuple2<Long,String>>) outgoing -> new Tuple2<>(outgoing.f2,outgoing.f0))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60L)))
                .apply((JoinFunction<Tuple6<String, String, Long, String, String, String>,
                        Tuple6<String, String, Long, String, String, Long>, RequestEvent>)
                        (incoming, outgoing) -> new RequestEvent(incoming.f1,outgoing.f1,incoming.f0,incoming.f2,incoming.f3,
                                incoming.f4,incoming.f5,outgoing.f3,outgoing.f4,outgoing.f5));

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName(params.getRequired(Constants.ES_HOST)),
                Integer.parseInt(params.getRequired(Constants.ES_PORT))));

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", params.getRequired(Constants.ES_CLUSTER_NAME));
        config.put("cluster.routing.allocation.enable", "all");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        resultStream.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<RequestEvent>() {
            IndexRequest createIndexRequest(RequestEvent element) {
                String suffix = element.getHost().toLowerCase();
                return Requests.indexRequest()
                        .index(params.getRequired(Constants.INDEX_NAME_PREFIX) + suffix)
                        .type("get-requests")
                        .source(element.toJson());
            }

            @Override
            public void process(RequestEvent element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));

        see.execute(params.getRequired(Constants.JOB_NAME));
    }
}
