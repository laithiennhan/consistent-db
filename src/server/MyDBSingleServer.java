package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.*;


/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {
    private final Cluster cluster;
    private final Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();
        this.session = cluster.connect(keyspace);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes);
        String response;
        String[] parts = request.split(":::", 2);
        String actualRequest = parts[0];
        String requestID = "";
        if (parts.length >= 2) {
            requestID = parts[1];
        }
        try {
            session.execute(actualRequest);
            response = "success:::" + requestID;
        } catch (Exception e) {
            response = "failed:::" + requestID;
        }

        try {
            log.log(Level.INFO, "{0} received message from {1}", new Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header.sndr});
            this.clientMessenger.send(header.sndr, response.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}