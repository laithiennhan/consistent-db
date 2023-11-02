package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.UUID;
import org.json.JSONException;
import org.json.JSONObject;

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
    private final AtomicInteger lamportClock = new AtomicInteger(0);
    private final PriorityBlockingQueue<Message> queue = new PriorityBlockingQueue<>();
    private final Map<String, Integer> ackMap = new ConcurrentHashMap<>();
    // Represent a message with its Lamport timestamp.
    private static class Message implements Comparable<Message> {
        byte[] content;
        int timestamp;
        String uniqueID;
        @Override
        public int compareTo(Message other) {
            return Integer.compare(this.timestamp, other.timestamp);
        }
    }

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

    // Message type enum
    private enum MessageType {
        MULTICAST_UPDATE,
        MULTICAST_ACK
    }

    // TODO: process bytes received from clients here
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // echo to client
        super.handleMessageFromClient(bytes, header);

        int ts = lamportClock.incrementAndGet();

        // Message message = new Message();
        // message.content = bytes;
        // message.timestamp = ts;
        // message.uniqueID = uniqueID;
        // ackMap.put(new String(bytes), 1); // Acknowledgment from this server
        String uniqueID = UUID.randomUUID().toString();
        ackMap.put(uniqueID, 0); // Acknowledgment from this server
        // multicast the update to all other servers
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
            // if (!node.equals(myID))
                try {
                    this.serverMessenger.send(node, createMulticastMessage(bytes, ts, uniqueID));
                } catch (IOException e) {
                    e.printStackTrace();
                }
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        log.log(Level.INFO, "{0} received relayed message from {1}",
                new Object[]{this.myID, header.sndr}); // simply log
        MessageType type = getMessageType(bytes);

        switch (type) {
            case MULTICAST_UPDATE:
                handleMulticastUpdate(bytes);
                break;
            case MULTICAST_ACK:
                handleMulticastAck(bytes);
                break;
        }
    }

    private void handleMulticastUpdate(byte[] bytes) {
        int receivedTimestamp = 0;
        JSONObject jsonObject = null;
        String messageID = "";
        try {
            jsonObject = new JSONObject(new String(bytes));
            receivedTimestamp = jsonObject.getInt("timestamp");
            messageID = jsonObject.getString("messageID");
        } catch (JSONException e) {
            // e.printStackTrace();
        }

        lamportClock.set(Math.max(lamportClock.get(), receivedTimestamp + 1)); // ?

        Message message = new Message();
        try {
            message.content = jsonObject.getString("payload").getBytes();
        } catch (JSONException e) {
            // e.printStackTrace();
        }
        message.timestamp = receivedTimestamp;
        message.uniqueID = messageID;
        queue.add(message);

        int tsAck = lamportClock.incrementAndGet();
        // multicast acknowledgment to all nodes
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
            // if (!node.equals(myID)) multicast to all nodes including itself
                try {
                    this.serverMessenger.send(node, createAckMessage(message.content, tsAck, messageID));
                } catch (IOException e) {
                    e.printStackTrace();
                }
    }

    private void handleMulticastAck(byte[] bytes) {
        int receivedTimestamp = 0;
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(new String(bytes));
            receivedTimestamp = jsonObject.getInt("timestamp");
        } catch (JSONException e) {
            // e.printStackTrace();
        }

        lamportClock.set(Math.max(lamportClock.get(), receivedTimestamp + 1));

        // String originalMessage = "";
        String messageID = "";
        try {
            // originalMessage = jsonObject.getString("payload");
            messageID = jsonObject.getString("messageID");
        } catch (JSONException e) {
            // e.printStackTrace();
        }
        ackMap.put(messageID, ackMap.getOrDefault(messageID, 0) + 1);

        if (allAcksReceivedFor(queue.peek())) {
            deliver(queue.poll());
        }
    }

    private boolean allAcksReceivedFor(Message message) {
        if (message == null) {
            return false;
        }

        // String originalMessage = new String(message.content);
        String messageID = message.uniqueID;
        int ackCount = ackMap.getOrDefault(messageID, 0);
        return ackCount == this.serverMessenger.getNodeConfig().getNodeIDs().size();
    }

    private void deliver(Message message) {
        String command = new String(message.content);
        session.execute(command);
        log.log(Level.INFO, "Delivered message with timestamp {0}", message.timestamp);
    }

    private MessageType getMessageType(byte[] bytes) {
        try {
            JSONObject jsonObject = new JSONObject(new String(bytes));
            return MessageType.valueOf(jsonObject.getString("type"));
        } catch (JSONException e) {
            // e.printStackTrace();
        }
        return null;
    }

    private byte[] createMulticastMessage(byte[] content, int timestamp, String messageID) {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("type", MessageType.MULTICAST_UPDATE.toString());
            jsonObject.put("payload", new String(content));
            jsonObject.put("timestamp", timestamp);
            jsonObject.put("messageID", messageID);
            return jsonObject.toString().getBytes();
        } catch (JSONException e) {
            // e.printStackTrace();
        }
        return null;
    }

    private byte[] createAckMessage(byte[] content, int timestamp, String messageID) {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("type", MessageType.MULTICAST_ACK.toString());
            jsonObject.put("payload", new String(content));
            jsonObject.put("timestamp", timestamp);
            jsonObject.put("messageID", messageID);
            return jsonObject.toString().getBytes();
        } catch (JSONException e) {
            // e.printStackTrace();
        }
        return null;
    }

    public void close() {
        super.close();
        session.close();
        cluster.close();
        this.serverMessenger.stop();
    }
}