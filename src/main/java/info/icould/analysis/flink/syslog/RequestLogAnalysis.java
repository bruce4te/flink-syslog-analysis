package info.icould.analysis.flink.syslog;

import info.icould.analysis.flink.domain.IncomingLine;
import info.icould.analysis.flink.domain.OutgoingLine;
import info.icould.analysis.flink.domain.RequestEvent;
import info.icould.analysis.flink.utils.Constants;
import info.icould.analysis.flink.utils.IncomingLineMapper;
import info.icould.analysis.flink.utils.JoinRequestFunction;
import info.icould.analysis.flink.utils.OutgoingLineMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(RequestLogAnalysis.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // make parameters available in the web interface
        see.getConfig().setGlobalJobParameters(params);
        Properties properties = setProperties(params);

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

        SplitStream<String> splitStream = splitLog(allRequestLogs);
        DataStream<IncomingLine> incomingStream = parseIncomingStream(splitStream);
        DataStream<OutgoingLine> outgoingStream = parseOutgoingStream(splitStream);
        DataStream<RequestEvent> resultStream = joinStreams(incomingStream, outgoingStream);

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName(params.getRequired(Constants.ES_HOST)),
                Integer.parseInt(params.getRequired(Constants.ES_PORT))));

        Map<String, String> config = setConfig(params);

        resultStream.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<RequestEvent>() {
            IndexRequest createIndexRequest(RequestEvent element) {
                String suffix = element.getHost().toLowerCase();
                LOG.info(element.toJson());
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

    private static SplitStream<String> splitLog(DataStream<String> allRequestLogs){
        return allRequestLogs.split((OutputSelector<String>) s -> {
            List<String> output = new ArrayList<>();
            if(s.contains("->")){
                output.add("incoming");
            } else {
                output.add("outgoing");
            }
            return output;
        });
    }

    private static DataStream<IncomingLine> parseIncomingStream(SplitStream<String> splitStream) {
        return splitStream
                .select("incoming")
                .map(new IncomingLineMapper());
    }

    private static DataStream<OutgoingLine> parseOutgoingStream(SplitStream<String> splitStream) {
        return splitStream
                .select("outgoing")
                .map(new OutgoingLineMapper());
    }

    private static DataStream<RequestEvent> joinStreams(DataStream<IncomingLine> incomingStream, DataStream<OutgoingLine> outgoingStream) {
        return incomingStream
                // join streams
                .join(outgoingStream)
                // where incoming.ID == outgoing.ID AND incoming.hostname == outgoing.hostname
                .where((KeySelector<IncomingLine, Tuple2<Long,String>>) incoming -> new Tuple2<>(incoming.getRequestId(),incoming.getHostName()))
                .equalTo((KeySelector<OutgoingLine, Tuple2<Long,String>>) outgoing -> new Tuple2<>(outgoing.getRequestId(), outgoing.getHostName()))
                // within 60s tumbling window
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60L)))
                // apply custom join function to form request event
                .apply(new JoinRequestFunction());
    }


    private static Properties setProperties(ParameterTool params) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.getRequired(Constants.KAFKA_BOOTSTRAP_SERVERS));
        return properties;
    }

    private static Map<String,String> setConfig(ParameterTool params) {
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", params.getRequired(Constants.ES_CLUSTER_NAME));
        config.put("cluster.routing.allocation.enable", "all");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        return config;
    }
}
