import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.nio.interfaces.NodeConfig;
import server.MyDBReplicatedServer;
import server.SingleServer;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class Grader extends GraderSingleServer {


    @BeforeClass
    public static void setup() throws IOException {
        // setup single server by calling parent setup
        GraderSingleServer.setup();

        // setup replicated servers and sessions to test
        replicatedServers = new SingleServer[nodeConfigServer.getNodeIDs().size()];
        int i = 0;
        // create keyspaces if not exists
        for (String node : nodeConfigServer.getNodeIDs()) {
            session.execute("create keyspace if not exists " + node + " with replication={'class':'SimpleStrategy', 'replication_factor' : '1'};");
        }

        for (String node : nodeConfigServer.getNodeIDs()) {
            replicatedServers[i++] = GRADING_MODE ? new MyDBReplicatedServer
                    (nodeConfigServer, node, DEFAULT_DB_ADDR) :

                    (SingleServer) getInstance(getConstructor("server" +
                                    ".AVDBReplicatedServer", NodeConfig.class,
                            String
                                    .class, InetSocketAddress.class),
                            nodeConfigServer, node, DEFAULT_DB_ADDR);
        }

        // setup frequently used information
        i = 0;
        servers = new String[nodeConfigServer.getNodeIDs().size()];
        for (String node : nodeConfigServer.getNodeIDs())
            servers[i++] = node;
        serverMap = new HashMap<String, InetSocketAddress>();
        for (String node : nodeConfigClient.getNodeIDs())
            serverMap.put(node, new InetSocketAddress(nodeConfigClient
                    .getNodeAddress
                            (node), nodeConfigClient.getNodePort(node)));

    }

    /**
     * In the replicated server tests below, each server operates in its own
     * keyspace. There is only one Cassandra server instance running.
     */

    /**
     * Create tables on all keyspaces.
     * Table should always exist after this test.
     * This test should always succeed, it is irrelevant to the total order implementation.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test10_CreateTables() throws IOException, InterruptedException {
        for (String node : servers) {
            // create default table, node is the keypsace name
            session.execute(getCreateTableWithList(DEFAULT_TABLE_NAME, node));
            Thread.sleep(SLEEP);
        }

        for (String node : servers) {
            verifyTableExists(DEFAULT_TABLE_NAME, node, true);
        }
    }

    /**
     * Select a single server and send all SQL queries to the selected server.
     * Then verify the results on all replicas to see whether they are consistent.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test11_UpdateRecord_SingleServer() throws IOException, InterruptedException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        String selected = servers[0];
        // insert a record first with an empty list
        client.send(serverMap.get(selected), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        Thread.sleep(SLEEP);

        for (int i = 0; i < servers.length; i++) {
            client.send(serverMap.get(selected), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));
            Thread.sleep(SLEEP);
        }

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }

    /**
     * Send a simple SQL query to every server in a round robin manner.
     * Then verify the results in all replicas to see whether they are consistent.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test12_UpdateRecord_AllServer() throws IOException, InterruptedException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        Thread.sleep(SLEEP);

        for (String node : servers) {
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));
            Thread.sleep(SLEEP);
        }

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }

    /**
     * Send each SQL query to a random server.
     * Then verify the results in all replicas to see whether they are consistent.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void test13_UpdateRecord_RandomServer() throws InterruptedException, IOException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        Thread.sleep(SLEEP);

        for (int i = 0; i < servers.length; i++) {
            String node = servers[ThreadLocalRandom.current().nextInt(0, servers.length)];
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));
            Thread.sleep(SLEEP);
        }

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }


    /**
     * This test is the same as test13, but it will send update request faster than test13, as it only sleeps 10ms
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void test14_UpdateRecordFaster_RandomServer() throws InterruptedException, IOException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        // this sleep is to guarantee that the record has been created
        Thread.sleep(SLEEP);

        for (int i = 0; i < servers.length; i++) {
            String node = servers[ThreadLocalRandom.current().nextInt(0, servers.length)];
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));
            // we just sleep 10 milliseconds this time
            Thread.sleep(10);
        }

        Thread.sleep(SLEEP);
        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }


    /**
     * This test is also the same as test13, but it will send update request much faster than test13, as it only sleeps 1ms
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void test15_UpdateRecordMuchFaster_RandomServer() throws InterruptedException, IOException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        // this sleep is to guarantee that the record has been created
        Thread.sleep(SLEEP);

        for (int i = 0; i < servers.length; i++) {
            String node = servers[ThreadLocalRandom.current().nextInt(0, servers.length)];
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));
            // we just sleep 10 milliseconds this time
            Thread.sleep(1);
        }
        Thread.sleep(SLEEP);

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }
    /**
     * Compared to the previous test, this test checks also for reliability,
     * i.e., all issued requests were completed.
     * @throws InterruptedException
     * @throws IOException
     */

    /**
     * This test will not sleep and send more requests faster.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void test16_UpdateRecordFastest_RandomServer() throws InterruptedException, IOException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        // this sleep is to guarantee that the record has been created
        Thread.sleep(SLEEP);

        for (int i = 0; i < NUM_REQS; i++) {
            String node = servers[ThreadLocalRandom.current().nextInt(0, servers.length)];
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));

        }

        Thread.sleep(SLEEP * NUM_REQS / SLEEP_RATIO);

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key);
    }

    @Test
    public void test17_UpdateRecordFastest_RandomServer_Reliably() throws InterruptedException, IOException {
        // generate a random key for this test
        int key = ThreadLocalRandom.current().nextInt();

        // insert a record first with an empty list, it doesn't matter which server we use, it should be consistent
        client.send(serverMap.get(servers[0]), insertRecordIntoTableCmd(key, DEFAULT_TABLE_NAME));
        // this sleep is to guarantee that the record has been created
        Thread.sleep(SLEEP);

        for (int i = 0; i < NUM_REQS; i++) {
            String node = servers[ThreadLocalRandom.current().nextInt(0, servers.length)];
            client.send(serverMap.get(node), updateRecordOfTableCmd(key, DEFAULT_TABLE_NAME));

        }

        Thread.sleep(SLEEP * NUM_REQS / SLEEP_RATIO);

        verifyOrderConsistent(DEFAULT_TABLE_NAME, key, true, NUM_REQS);
    }

    @AfterClass
    public static void teardown() {
        for (String node : servers)
            // clean up tables
            session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, node));

        if (replicatedServers != null)
            for (SingleServer s : replicatedServers)
                s.close();
        GraderSingleServer.teardown();
    }


    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(Grader.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }
}
