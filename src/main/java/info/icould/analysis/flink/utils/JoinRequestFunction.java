package info.icould.analysis.flink.utils;

import info.icould.analysis.flink.domain.IncomingLine;
import info.icould.analysis.flink.domain.OutgoingLine;
import info.icould.analysis.flink.domain.RequestEvent;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinRequestFunction implements JoinFunction<IncomingLine, OutgoingLine, RequestEvent> {
    @Override
    public RequestEvent join(IncomingLine incoming, OutgoingLine outgoing){
        return new RequestEvent(incoming, outgoing);
    }
}
