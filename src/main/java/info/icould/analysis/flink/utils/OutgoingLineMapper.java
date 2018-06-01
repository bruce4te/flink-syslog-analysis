package info.icould.analysis.flink.utils;

import info.icould.analysis.flink.domain.OutgoingLine;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class OutgoingLineMapper implements MapFunction<String, OutgoingLine> {

  private static final Logger LOG = LoggerFactory.getLogger(OutgoingLineMapper.class);

  @Override
  public OutgoingLine map(String s) {
      String[] lineParts = s.split(" ");
      LOG.info("outgoing: {}",s);
      DateTimeFormatter accessFormatter = DateTimeFormatter.ofPattern(Constants.ACCESS_LOG_DF, Locale.ENGLISH);
      LocalDateTime logDate = LocalDateTime.parse(lineParts[2], accessFormatter);
      String timestamp = logDate.toString();
      long requestId = Long.parseLong(lineParts[4].replace("[","").replace("]", ""));
      long duration = Long.parseLong(lineParts[lineParts.length-1].replace("ms",""));
      return new OutgoingLine(lineParts[1],timestamp,
              requestId,lineParts[6],lineParts[7],duration);
  }
}
