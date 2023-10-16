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

        try {
            ResultSet results = session.execute(request);
            // For simplicity, just returning the first row as response
            if (!results.isExhausted()) {
                Row row = results.one();
                response = row.toString();
            } else {
                response = "Operation executed successfully, but no data to return.";
            }
        } catch (Exception e) {
            response = "Error executing operation: " + e.getMessage();
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