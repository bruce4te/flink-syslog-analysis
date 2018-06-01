package info.icould.analysis.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import static info.icould.analysis.flink.utils.Constants.ACCESS_LOG_DF;
import static java.lang.String.format;

public class LogLineMapper implements MapFunction<String, Tuple4<String, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(LogLineMapper.class);
    private final String regex;
    private final int valuePos;
    private final int datetimePos;
    private final int hostPos;
    private final int directionPos;

    public LogLineMapper(String regex, int hostPos, int datetimePos, int directionPos, int valuePos){
        this.regex = regex;
        this.valuePos = valuePos;
        this.datetimePos = datetimePos;
        this.hostPos = hostPos;
        this.directionPos = directionPos;
    }

    @Override
    public Tuple4<String, String, String, String> map(String s) {
        s = s.replace("  "," ");
        String[] logLineParts = s.split(this.regex);
        if (LOG.isInfoEnabled()) {
            LOG.info("Log Line: {}", s);
        }
        String logLineValue = logLineParts[this.valuePos];
        String host = logLineParts[this.hostPos];
        String direction = logLineParts[this.directionPos];
        Tuple4<String,String,String,String> result;
        DateTimeFormatter accessFormatter = DateTimeFormatter.ofPattern(ACCESS_LOG_DF, Locale.ENGLISH);
        try {
            LocalDateTime logDate = LocalDateTime.parse(logLineParts[this.datetimePos], accessFormatter);
            LocalDateTime localDateTime = logDate.plusMinutes(1L);
            localDateTime = localDateTime.minusSeconds(logDate.getSecond());
            result = new Tuple4<>(localDateTime.toString(), logLineValue, host, direction);
            if (LOG.isInfoEnabled()) {
                LOG.info("Map result: {}", result);
            }
        } catch (DateTimeParseException dtpe){
            LOG.error(format("Error parsing date: %s, %s", logLineParts[this.datetimePos], logLineParts));
            result = new Tuple4<>("Date parsing exception", logLineValue, host, direction);
        }
        return result;
    }
}
