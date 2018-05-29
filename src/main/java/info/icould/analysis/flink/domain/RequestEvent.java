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

    public RequestEvent(String incomingTStamp, String outgoingTStamp, String host, long requestId, String method,
                        String path, String protocol, String code, String mimeType, long duration) {
        this.incomingTStamp = incomingTStamp;
        this.outgoingTStamp = outgoingTStamp;
        this.host = host;
        this.requestId = requestId;
        this.method = method;
        this.path = path;
        this.protocol = protocol;
        this.code = code;
        this.mimeType = mimeType;
        this.duration = duration;
    }

    public String getHost() {
        return host;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
