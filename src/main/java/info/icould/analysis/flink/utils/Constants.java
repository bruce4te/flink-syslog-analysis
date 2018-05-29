package info.icould.analysis.flink.utils;

public class Constants {

    private Constants() {
        throw new IllegalStateException("Utility class");
    }

    public static final String ACCESS_LOG_DF = "dd/MMM/yyyy:HH:mm:ss";
    public static final String ES_CLUSTER_NAME = "ec";
    public static final String ES_HOST = "eh";
    public static final String ES_PORT = "ep";
    public static final String ES_ADDRESS = "ea";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kbs";
    public static final String KAFKA_TOPIC = "kt";
    public static final String INDEX_NAME_PREFIX = "in";
    public static final String JOB_NAME = "jn";
}
