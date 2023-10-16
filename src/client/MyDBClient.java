package client;

import edu.umass.cs.nio.interfaces.NodeConfig;
import server.SingleServer;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashMap;

public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig = null;

    private final HashMap<String, Callback> callbackMap = new HashMap<String, Callback>();

    public MyDBClient() throws IOException {
        super();
    }

    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        try {
            String response = new String(bytes, SingleServer.DEFAULT_ENCODING);
            String[] parts = response.split(":::", 2);
            String actualResponse = parts[0];
            String requestID = parts[1];

            if  (requestID.length() > 0) {
                Callback callback = callbackMap.remove(requestID);
                if (callback != null) {
                    callback.handleResponse(actualResponse.getBytes(SingleServer.DEFAULT_ENCODING), header);
                } else {
                    super.handleResponse(bytes, header);
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback callback) throws IOException {
        String requestID = String.valueOf(System.currentTimeMillis());
        String modifiedRequest = request + ":::" + requestID;
        callbackMap.put(requestID, callback);
        send(isa, modifiedRequest);
    }
}
