package info.icould.analysis.flink.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SyslogSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(SyslogSource.class);

    private static final long serialVersionUID = 1L;
    private static final int BUF_SIZE = 1024;
    private static final int QUEUE_SIZE = 10000;

    private transient BlockingQueue<String> queue;
    private transient DatagramSocket socket;
    private transient volatile boolean isRunning;

    private final String host;
    private final int port;

    private final int queueSize;
    private byte[] buf = new byte[BUF_SIZE];

    public SyslogSource(String host, int port) {
        this(host, port, QUEUE_SIZE);
    }

    private SyslogSource(String host, int port, int queueSize) {
        this.isRunning = false;
        this.host = host;
        this.port = port;
        this.queueSize = queueSize;
    }

    @Override
    public void open(Configuration parameters) {
        initializeConnection();
    }

    private void initializeConnection() {

        if ( socket != null && socket.isBound() )
            socket.close();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("Initializing Syslog Streaming connection on port: " + port);
            }
            InetAddress address = InetAddress.getByName(host);
            socket = new DatagramSocket(port, address);
            isRunning = true;
        } catch (java.net.SocketException se) {
            LOG.error("Error creating socket: ", se.getMessage());
        } catch (UnknownHostException uhe) {
            LOG.error("Error connecting to host: ", uhe.getMessage());
        }
        queue = new LinkedBlockingQueue<>(getQueueSize());
    }



    private void receivePacket() {
        try {
            DatagramPacket packet = new DatagramPacket(buf, BUF_SIZE);
            socket.receive(packet);
            byte[] received = new byte[packet.getLength()];

            //copy received packet from buf to received
            System.arraycopy(buf, 0, received, 0, packet.getLength());
            queue.add(packet.getAddress().getHostAddress() + ":" + new String(received, StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            LOG.error("Error receiving packet: ", e);
        }
    }

    @Override
    public void close() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Initiating socket connection close");
        }
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            //can't do much if we can't close it ...
        }
        socket = null;

        if (LOG.isInfoEnabled()) {
            LOG.info("Socket connection closed successfully");
        }
    }

    @Override
    public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            if(socket==null) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Client connection closed unexpectedly: {}", "Syslog");
                }
                break;
            } else {
                receivePacket();
                sourceContext.collect(queue.take());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private int getQueueSize() {
        return queueSize;
    }

}