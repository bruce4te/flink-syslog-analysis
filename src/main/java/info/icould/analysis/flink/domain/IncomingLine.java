package info.icould.analysis.flink.domain;

import com.google.gson.Gson;

public class IncomingLine {
  private String hostName;
  private String eventTime;
  private Long requestId;
  private String method;
  private String path;
  private String protocol;

  public IncomingLine(String hostName, String eventTime, Long requestId, String method, String path, String protocol) {
    this.hostName = hostName;
    this.eventTime = eventTime;
    this.requestId = requestId;
    this.method = method;
    this.path = path;
    this.protocol = protocol;
  }

  public String getHostName() {
    return hostName;
  }

  public String getEventTime() {
    return eventTime;
  }

  public Long getRequestId() {
    return requestId;
  }

  public String getMethod() {
    return method;
  }

  public String getPath() {
    return path;
  }

  public String getProtocol() {
    return protocol;
  }

  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
