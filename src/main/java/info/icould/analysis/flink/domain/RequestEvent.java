package info.icould.analysis.flink.domain;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class RequestEvent {
    //Apr  5 12:44:36 aem-author 05/Apr/2018:12:44:36 +0000 [370] -> GET /mnt/overlay/dam/gui/content/assets/jcr:content/views/card.0.20.html/content/dam/we-retail-client-app?_=1522932155825 HTTP/1.1
    //        4> <14>Apr  5 12:44:36 aem-author 05/Apr/2018:12:44:36 +0000 [370] <- 200 text/html 9ms
    @SerializedName("@timestamp")
    private String incomingTStamp;
    @SerializedName("out_timestamp")
    private String outgoingTStamp;
    private String host;
    @SerializedName("request_id")
    private long requestId;
    private String method;
    private String path;
    private String protocol;
    private String code;
    @SerializedName("mime_type")
    private String mimeType;
    private long duration;

    public RequestEvent(IncomingLine incoming, OutgoingLine outgoing) {
        this.incomingTStamp = incoming.getEventTime();
        this.outgoingTStamp = outgoing.getEventTime();
        this.host = incoming.getHostName();
        this.requestId = incoming.getRequestId();
        this.method = incoming.getMethod();
        this.path = incoming.getPath();
        this.protocol = incoming.getProtocol();
        this.code = outgoing.getResponseCode();
        this.mimeType = outgoing.getMimeType();
        this.duration = outgoing.getDuration();
    }

    public String getHost() {
        return host;
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

    public String getCode() {
        return code;
    }

    public long getDuration() {
        return duration;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
