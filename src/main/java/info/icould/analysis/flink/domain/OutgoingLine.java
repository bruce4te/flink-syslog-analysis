package info.icould.analysis.flink.domain;

import com.google.gson.Gson;

public class OutgoingLine {
  private String hostName;
  private String eventTime;
  private Long requestId;
  private String responseCode;
  private String mimeType;
  private Long duration;

  public OutgoingLine(String hostName, String eventTime, Long requestId, String responseCode, String mimeType, Long duration) {
    this.hostName = hostName;
    this.eventTime = eventTime;
    this.requestId = requestId;
    this.responseCode = responseCode;
    this.mimeType = mimeType;
    this.duration = duration;
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

  public String getResponseCode() {
    return responseCode;
  }

  public String getMimeType() {
    return mimeType;
  }

  public Long getDuration() {
    return duration;
  }

  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
