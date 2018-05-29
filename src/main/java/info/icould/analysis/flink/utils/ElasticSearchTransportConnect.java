package info.icould.analysis.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ElasticSearchTransportConnect {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchTransportConnect.class);

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String[] addresses = {params.getRequired(Constants.ES_ADDRESS)};
        String clusterName = params.getRequired(Constants.ES_CLUSTER_NAME);
        // create default settings and add cluster name
        Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.enable", "all")
                .put("cluster.routing.allocation.allow_rebalance", "always");
        settings.put("cluster.name", clusterName);

        // create a client
        try (TransportClient tc = new PreBuiltTransportClient(settings.build())){
            for (String address : addresses) {
                String a = address.trim();
                int p = a.indexOf(':');
                if (p >= 0) try {
                    InetAddress i = InetAddress.getByName(a.substring(0, p));
                    int port = Integer.parseInt(a.substring(p + 1));
                    tc.addTransportAddress(new InetSocketTransportAddress(i, port));
                } catch (UnknownHostException e) {
                    LOG.warn("", e);
                }
            }

            List<DiscoveryNode> nodes = tc.connectedNodes();
            for(DiscoveryNode node:nodes){
                LOG.info(node.getHostName());
            }
        }
    }
}