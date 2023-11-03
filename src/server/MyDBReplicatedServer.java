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
    protected final MessageNIOTransport<String, String> serverMessenger;
    final private Session session;
    final private Cluster cluster;
    private final AtomicInteger lamportClock = new AtomicInteger(0);
    private final PriorityBlockingQueue<Message> queue = new PriorityBlockingQueue<>();
    private final Map<String, Integer> ackMap = new ConcurrentHashMap<>();
    // key: sender node id, value: expected seq
    private final Map<String, Integer> sequenceMap = new ConcurrentHashMap<>();
    // store commands yet to be executed
    private final Map<String, PriorityBlockingQueue<Message>> buffer = new ConcurrentHashMap<>();
    // sequence number currently in this node:
    private final AtomicInteger sequence = new AtomicInteger(0);

    // Represent a message with its Lamport timestamp.
    private static class Message implements Comparable<Message> {
        byte[] content;
        int timestamp;
        String uniqueID;
        int senderSequence;
        MessageType type;

        @Override
        public int compareTo(Message other) {
            return Integer.compare(this.timestamp, other.timestamp);
        }
    }

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
            InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        handleMessageFromServer(bytes, nioHeader);
                        return true;
                    }
                }, true);
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();
        this.session = cluster.connect(myID);

        // initialize the sequence map
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
            // if (!node.equals(myID))
            sequenceMap.put(node, 0);
            buffer.put(node, new PriorityBlockingQueue<Message>());
        }

        // log.log(Level.INFO, "Server {0} started on {1}",
        //         new Object[] { this.myID, this.clientMessenger.getListeningSocketAddress() });
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
        synchronized (sequence) {
            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                // if (!node.equals(myID))
                try {
                    this.serverMessenger.send(node, createMulticastMessage(bytes, ts, uniqueID));
                    log.log(Level.INFO, "Receiver {0}",
                            new Object[] { node }); // simply log
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            sequence.getAndIncrement();
        }
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // log.log(Level.INFO, "At hmfs, {0} received relayed message from {1}",
        //         new Object[] { this.myID, header.sndr }); // simply log

        MessageType type = getMessageType(bytes);
        int senderSequence;
        String senderID;
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(new String(bytes));
            senderSequence = jsonObject.getInt("sequenceNumber");
            // senderSequence = Integer.parseInt(jsonObject.getString("sequenceNumber"));
            senderID = jsonObject.getString("senderID");
            int receivedTimestamp = jsonObject.getInt("timestamp");
            String messageID = jsonObject.getString("messageID");
            // int expectedSequence = sequenceMap.get(senderID);
            byte[] payload = jsonObject.getString("payload").getBytes();
            // String uniqueID = jsonObject.getString("uniqueID");
            // log.log(Level.INFO, "Sender Sequence: {0}, Expected sequence: {1}, SenderID: {2}, MyID: {3}",
            //         new Object[] { senderSequence, expectedSequence, senderID, myID}); // simply log
            // if (senderSequence != expectedSequence) {
            //     Message message = new Message();
            //     message.content = payload;
            //     message.timestamp = receivedTimestamp;
            //     message.senderSequence = senderSequence;
            //     message.uniqueID = messageID;
            //     message.type = type;
            //     buffer.get(senderID).add(message);
            // } else {
            //     switch (type) {
            //         case MULTICAST_UPDATE:
            //             handleMulticastUpdate(bytes);
            //             break;
            //         case MULTICAST_ACK:
            //             handleMulticastAck(bytes);
            //             break;
            //     }
            //     expectedSequence++;
            //     sequenceMap.put(senderID, expectedSequence);
            //     while (!buffer.get(senderID).isEmpty() && buffer.get(senderID).peek().senderSequence == expectedSequence) {
            //         Message nextMessage = buffer.get(senderID).poll();
            //         // byte[] nextBytes = nextMessage.content;
            //         // MessageType nextType = getMessageType(nextBytes);
            //         JSONObject nextJSONObject = new JSONObject();
            //         nextJSONObject.put("type", nextMessage.type.toString());
            //         nextJSONObject.put("payload", new String(nextMessage.content));
            //         nextJSONObject.put("timestamp", nextMessage.timestamp);
            //         nextJSONObject.put("messageID", nextMessage.uniqueID);
            //         switch (nextMessage.type) {
            //             case MULTICAST_UPDATE:
            //                 handleMulticastUpdate(nextJSONObject.toString().getBytes());
            //                 break;
            //             case MULTICAST_ACK:
            //                 handleMulticastAck(nextJSONObject.toString().getBytes());
            //                 break;
            //         }
            //         expectedSequence++;
            //         sequenceMap.put(senderID, expectedSequence);
            //     }
            // }

            Message message = new Message();
            message.content = payload;
            message.timestamp = receivedTimestamp;
            message.senderSequence = senderSequence;
            message.uniqueID = messageID;
            message.type = type;
            // if (!sequenceMap.containsKey(senderID)) {
            //     sequenceMap.put(senderID, 0);
            // }
            // if (!buffer.containsKey(senderID)) {
            //     buffer.put(senderID, new PriorityBlockingQueue<Message>());
            // }
            buffer.get(senderID).add(message);

            while (!buffer.get(senderID).isEmpty() && buffer.get(senderID).peek().senderSequence == sequenceMap.get(senderID)) {
                Message nextMessage = buffer.get(senderID).poll();
                // byte[] nextBytes = nextMessage.content;
                // MessageType nextType = getMessageType(nextBytes);
                JSONObject nextJSONObject = new JSONObject();
                nextJSONObject.put("type", nextMessage.type.toString());
                nextJSONObject.put("payload", new String(nextMessage.content));
                nextJSONObject.put("timestamp", nextMessage.timestamp);
                nextJSONObject.put("messageID", nextMessage.uniqueID);
                switch (nextMessage.type) {
                    case MULTICAST_UPDATE:
                        handleMulticastUpdate(nextJSONObject.toString().getBytes());
                        break;
                    case MULTICAST_ACK:
                        handleMulticastAck(nextJSONObject.toString().getBytes());
                        break;
                }
                sequenceMap.put(senderID, sequenceMap.get(senderID) + 1);
            }
        } catch (JSONException e) {
            // e.printStackTrace();
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

        lamportClock.set(Math.max(lamportClock.get(), receivedTimestamp) + 1); // ?

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
        synchronized (sequence) {
            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                // if (!node.equals(myID)) multicast to all nodes including itself
                try {
                    this.serverMessenger.send(node, createAckMessage(message.content, tsAck, messageID));
                    log.log(Level.INFO, "Receiver {0}",
                            new Object[] { node }); // simply log
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            sequence.getAndIncrement();
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

        lamportClock.set(Math.max(lamportClock.get(), receivedTimestamp) + 1);

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
        // log.log(Level.INFO, "Number of nodes {0}",
        //         new Object[] { this.serverMessenger.getNodeConfig().getNodeIDs().size() }); // simply log
        return ackCount == this.serverMessenger.getNodeConfig().getNodeIDs().size();
    }

    private void deliver(Message message) {
        String command = new String(message.content);
        session.execute(command);
        // log.log(Level.INFO, "Delivered message with timestamp {0}", message.timestamp);
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
            jsonObject.put("senderID", myID);
            jsonObject.put("sequenceNumber", sequence.get());
            log.log(Level.INFO, "messageID {0}, sequenceNumber {1}, senderID {2}, type {3}",
                    new Object[] { messageID, sequence.get(), myID, MessageType.MULTICAST_UPDATE.toString() }); // simply log
            // jsonObject.put("sequenceNumber", sequence.getAndIncrement());

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
            jsonObject.put("senderID", myID);
            jsonObject.put("sequenceNumber", sequence.get());
            log.log(Level.INFO, "messageID {0}, sequenceNumber {1}, senderID {2}, type {3}",
                    new Object[] { messageID, sequence.get(), myID, MessageType.MULTICAST_ACK.toString() }); // simply log
            // jsonObject.put("sequenceNumber", sequence.getAndIncrement());
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