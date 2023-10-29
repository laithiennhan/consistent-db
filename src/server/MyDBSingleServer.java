package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.*;

import client.MyDBClient;


/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {
    final private Session session;
    final private Cluster cluster;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect("demo");
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // simple echo server
        try {
            log.log(Level.INFO, "{0} received message from {1} {2}", new
                    Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header
                            .sndr, new String(bytes)});
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            JSONObject json = null;
            try {
                json = new JSONObject(request);
                request = json.getString(MyDBClient.Keys.REQUEST
                        .toString());
            } catch (JSONException e) {
                //e.printStackTrace();
            }

            ResultSet results = session.execute(request);
            String response="";
            for(Row row : results) {
                response += row.toString();
            }
            if(json!=null) try {
                json.put(MyDBClient.Keys.RESPONSE.toString(),
                        response);
                response = json.toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            System.out.println(this.clientMessenger
                    .getListeningSocketAddress() + " executed " +
                    request + " and sending response " +"["+response+"]");
            this.clientMessenger.send(header.sndr, response.getBytes
                    (SingleServer.DEFAULT_ENCODING));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        super.close();
        session.close();
        cluster.close();
    }
}