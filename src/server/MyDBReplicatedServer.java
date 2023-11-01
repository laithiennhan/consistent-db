package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

import com.datastax.driver.core.*;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends SingleServer {

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;
    final private Session session;
    final private Cluster cluster;

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID)-ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.serverMessenger = new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();
        this.session = cluster.connect(myID);
        log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress()});
    }

    // TODO: process bytes received from clients here
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // echo to client
        super.handleMessageFromClient(bytes, header);

        // relay to other servers
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
            if (!node.equals(myID))
                try {
                    this.serverMessenger.send(node, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
        // Broadcast intent to send message to other servers
        // Wait for all other ACKs with timestamps
        // Select highest proposed timestamp
        // Deliver message with timestamp selected above to other servers
        // Push message with timestamp selected above to priority queue
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        log.log(Level.INFO, "{0} received relayed message from {1}",
                new Object[]{this.myID, header.sndr}); // simply log
        
        // When receive intent: increment clock and send ACK with local clock counter
        // When receive message: adjust timestamp (Lamport algo cai max cua e Thinh), increment clock and add message + timestamp to priority queue


    }

    public void close() {
        super.close();
        session.close();
        cluster.close();
        this.serverMessenger.stop();
    }
}