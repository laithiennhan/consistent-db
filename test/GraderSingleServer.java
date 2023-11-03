import client.Client;
import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.gradescope.jh61b.grader.GradedTest;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.DefaultTest;
import org.junit.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import server.MyDBSingleServer;
import server.ReplicatedServer;
import server.SingleServer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderSingleServer extends DefaultTest {

    protected static final String DEFAULT_KEYSPACE = "demo";
    protected static final InetSocketAddress DEFAULT_SADDR = new InetSocketAddress
            ("localhost", 1999);
    protected static final InetSocketAddress DEFAULT_DB_ADDR = new
            InetSocketAddress("localhost", 9042);
    protected static final int NUM_SERVERS = 3;
    protected static final int SLEEP = 1000;
    protected static final String CONFIG_FILE = System.getProperty("config")
            != null ? System.getProperty("config") :
            "conf/servers.properties";

    protected static Client client = null;
    protected static SingleServer singleServer = null;
    protected static SingleServer[] replicatedServers = null;
    protected static Map<String, InetSocketAddress> serverMap = null;
    protected static String[] servers = null;
    protected static Cluster cluster;
    protected static Session session = (cluster = Cluster.builder().addContactPoint
            (DEFAULT_SADDR
                    .getHostName()).build()).connect(DEFAULT_KEYSPACE);

    protected final static String DEFAULT_TABLE_NAME = "grade";

    protected static NodeConfig<String> nodeConfigServer;

    protected static NodeConfig<String> nodeConfigClient;
    protected static final boolean GRADING_MODE = true;

    protected static final int NUM_REQS = 100;

    protected static final int SLEEP_RATIO = 10;

    @BeforeClass
    public static void setup() throws IOException {
        // setup single server
        singleServer = GRADING_MODE ? new MyDBSingleServer(DEFAULT_SADDR,
                DEFAULT_DB_ADDR, DEFAULT_KEYSPACE) :
                (SingleServer) getInstance(getConstructor("server" +
                                ".AVDBSingleServer",
                        InetSocketAddress.class, InetSocketAddress.class,
                        String.class), DEFAULT_SADDR, DEFAULT_DB_ADDR,
                        DEFAULT_KEYSPACE);
        nodeConfigServer = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET);

        nodeConfigClient = NodeConfigUtils.getNodeConfigFromFile
                (CONFIG_FILE, ReplicatedServer.SERVER_PREFIX);
        // setup client
        client = GRADING_MODE ? new MyDBClient(nodeConfigClient) :
                (Client) getInstance(getConstructor("client.AVDBClient",
                        NodeConfig.class),
                        nodeConfigClient);

    }

    /**
     * This test sends a simple default DB command expected to always succeed
     * because we are not checking that the server does anything useful.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @GradedTest(name = "test01_DefaultAsync()", max_score = 10)
    public void test01_DefaultAsync() throws IOException,
            InterruptedException {
        client.send(DEFAULT_SADDR, "select table_name from system_schema" +
                ".tables");
    }

    /**
     * Tests that a table is indeed being created successfully. This test
     * checks that the server processes the received command correctly by
     * relaying to the DB.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @GradedTest(name = "test02_Single_CreateTable_Async()", max_score = 10)
    public void test02_Single_CreateTable_Async() throws IOException,
            InterruptedException {
        dropTableIfExists();
        testCreateTable(true, true);
    }


    /**
     * This test is similar to the test above and verifies that the server
     * correctly processes commands to insert records. If the server passes
     * the previous test, it should also pass this test, i.e., no new logic
     * is needed for this test.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @GradedTest(name = "test03_InsertRecords_Async()", max_score = 10)
    public void test03_InsertRecords_Async() throws IOException, InterruptedException {
        int numInserts = 10;
        clearTableRecords();
        for (int i = 0; i < numInserts; i++) {
            send("insert into " + TABLE + " (ssn, firstname, lastname) " +
                    "values (" + (int) (Math.random() * Integer.MAX_VALUE) + ", '" +
                    "John" + i + "', '" + "Smith" + i + "')", true);
        }
        Thread.sleep(SLEEP);
        verifyInsertedNumRecordsExist(TABLE, DEFAULT_KEYSPACE, numInserts);
    }

    /**
     *
     * Same as above for deleting records. No new logic needed.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @GradedTest(name = "test04_DeleteRecords_Async()", max_score = 10)
    public void test04_DeleteRecords_Async() throws IOException, InterruptedException {
        send("truncate users", true);
        Thread.sleep(SLEEP);
        ResultSet resultSet = session.execute(getTableCountCmd(TABLE,DEFAULT_KEYSPACE));
        Assert.assertTrue(!resultSet.isExhausted());
        Assert.assertEquals(0, resultSet.one().getLong(0));
    }

    /**
     * This test checks if the client implements support for callbacks
     * correctly. It first deletes the table and then issues a command to
     * create the table along with a callback function expected to be invoked
     * by the client when it gets a confirmation of the command's successful
     * execution back from the server.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @GradedTest(name = "test05_CreateTable_Sync()", max_score = 10)
    public void test05_CreateTable_Sync() throws IOException, InterruptedException {
        // executing directly as opposed to sending via the client ensures
        // that the drop table command doesn't get reordered and executed
        // after the create.
        session.execute(getDropTableCmd(TABLE, DEFAULT_KEYSPACE));
        testCreateTable(true, false);
    }

    @Test
    @GradedTest(name = "test06_MultipleOperations_Sync()", max_score = 10)
    public void test06_MultipleOperations_Sync() throws IOException,
            InterruptedException {
        session.execute(getCreateTableCmd(TABLE, DEFAULT_KEYSPACE));

        // add records
        int numInserts = 10;
        clearTableRecords();

        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(numInserts);
        for (int i = 0; i < numInserts; i++) {
            final int j = i;
            // wait and check
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        int ssn = (int) (Math.random() * Integer.MAX_VALUE);
                        waitResponse(callbackSend(DEFAULT_SADDR, "insert into "
                                + DEFAULT_KEYSPACE + "." + TABLE + " " +
                                "(ssn, " + "firstname, " + "lastname) " +
                                "values (" + ssn + ", '" + "John" + j + "', " +
                                "'" + "Smith" + j + "')"));
                        ResultSet results = session.execute("select ssn from "
                                + TABLE + " " + "where " + "ssn=" + ssn);
                        Assert.assertTrue(results.one().getInt(0) == ssn);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                }
            });
        }
        while(!outstanding.isEmpty());
        clearTableRecords();
        executor.shutdown();
    }




    protected void testCreateTable(boolean single, boolean sleep) throws
            IOException, InterruptedException {
        if (sleep) testCreateTableSleep(single);
        else testCreateTableBlocking(single);
        verifyTableExists(TABLE, DEFAULT_KEYSPACE, true);

    }

	/**
	 * This method will directly contact the DB and drop the table.
     * @throws IOException
     */
    protected void dropTableIfExists() throws IOException {
        session.execute(getDropTableCmd(TABLE, DEFAULT_KEYSPACE));
    }

    protected void clearTableRecords() throws IOException {
        session.execute(getTruncateTableCmd(TABLE, DEFAULT_KEYSPACE));
    }

    protected void verifyTableExists(String table, String keyspace, boolean exists) {
        ResultSet resultSet = session.execute("select table_name from " +
                "system_schema.tables where keyspace_name='" + keyspace + "'");
        Assert.assertTrue(!resultSet.isExhausted());
        boolean match = false;
        for (Row row : resultSet)
            match = match || row.getString("table_name").equals(table);
        if (exists)
            Assert.assertTrue(match);
        else
            Assert.assertFalse(match);
    }

    protected void verifyInsertedNumRecordsExist(String table, String keyspace,
                                               int numRequests) {
        ResultSet resultSet = session.execute("select count(*) from " +
                 keyspace + "." + table);
        Assert.assertTrue(!resultSet.isExhausted());
        boolean match = false;
        match = match || (resultSet.one().getLong(0) == numRequests);
        Assert.assertTrue(match);
    }

    protected void verifyOrderConsistent(String table, int key,
										 boolean strict, int numExpected) {
        ArrayList<Integer>[] results = new ArrayList[servers.length];
        int i = 0;
        boolean nonEmpty = false;
        for (String node : servers) {
            ResultSet result = session.execute(readResultFromTableCmd(key, DEFAULT_TABLE_NAME, node));
            results[i] = new ArrayList<Integer>();
            for (Row row : result) {
                results[i] = new ArrayList<Integer>(row.getList("events",
                        Integer.class));
                nonEmpty = true;
            }
            i++;
        }

        i=0;
        int longestListIndex = 0;
        int longestListSize = 0;
        for(int j=0; j<results.length; j++) {
            if(results[j].size() > longestListSize) {
				longestListIndex = j;
				longestListSize = results[j].size();
			}
        }

        i = 0;
        boolean match = true;
		String message="[\n";
        for (ArrayList<Integer> result : results) {
            for(i=0; i<result.size(); i++){
				// prefix match
                if (!result.get(i).equals(results[longestListIndex].get(i))) {
					match = false;
					message += result + "\n!=(prefix mismatch at location " + i +
				": " + result.get(i) + "!=" + results[longestListIndex].get(i) +
							")\n" + results[longestListIndex] +")\n";
					break;
				}
				// prefix match and sequence size match
				if(strict && result.size() != results[longestListIndex].size()) {
					match = false;
					message += "result size " + result.size() + "   !=  " +
						"longestSize=" + results[longestListIndex].size() + "\n";
					break;
				}
				// prefix match and sequence size == numExpected
				if(strict && numExpected > 0 && result.size() != numExpected) {
					match = false;
					message += "result size " + result.size() + "  !=  " +
						"numExpected " + numExpected + "\n";
					break;
				}

            }
			if(!match) break;
        }
        for(i=0; i<results.length; i++)
            System.out.println(i+":"+results[i]);
        Assert.assertTrue(message, nonEmpty && match);
    }

	protected void verifyOrderConsistent(String table, int key) {
		this.verifyOrderConsistent(table, key, false, 0);
	}

		protected void testCreateTableSleep(boolean single) throws
            InterruptedException, IOException {
        send(getDropTableCmd(TABLE, DEFAULT_KEYSPACE), single);
        Thread.sleep(SLEEP);
        send(getCreateTableCmd(TABLE, DEFAULT_KEYSPACE), single);
        Thread.sleep(SLEEP);
    }


    ConcurrentHashMap<Long, String> outstanding = new ConcurrentHashMap<Long, String>();

    protected void testCreateTableBlocking(boolean single) throws
            InterruptedException, IOException {
        waitResponse(callbackSend(DEFAULT_SADDR, getDropTableCmd(TABLE, DEFAULT_KEYSPACE)));
        waitResponse(callbackSend(DEFAULT_SADDR, getCreateTableCmd(TABLE, DEFAULT_KEYSPACE)));
    }

    protected Long callbackSend(InetSocketAddress isa, String request) throws
            IOException {
        Long id = enqueueRequest(request);
        client.callbackSend(isa, request, new WaitCallback(id));
        return id;
    }

    protected class WaitCallback implements Client.Callback {
        Long monitor; // both id and monitor

        WaitCallback(Long monitor) {
            this.monitor = monitor;
        }

        @Override
        public void handleResponse(byte[] bytes, NIOHeader header) {
            synchronized (this.monitor) {
                outstanding.remove(monitor);
                this.monitor.notify();
            }
        }
    }

    protected long reqnum = 0;

    protected long enqueue() {
        synchronized (outstanding) {
            return reqnum++;
        }
    }

    protected long enqueueRequest(String request) {
        long id = enqueue();
        outstanding.put(id, request);
        return id;
    }

    protected void waitResponse(Long id) {
        synchronized (id) {
            while (outstanding.containsKey(id))
                try {
                    id.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }


    protected static final void send(String cmd, boolean single) throws
            IOException {
        client.send(single ? DEFAULT_SADDR :
                        serverMap.get(servers[(int) (Math.random() * servers.length)]),
                cmd);
    }

    protected static final String TABLE = "users";

    protected static String getCreateTableCmd(String table, String keyspace) {
        return "create table if not exists " + keyspace + "." + table + " (age int, firstname " +
                "text, lastname text, ssn int, address text, hash bigint, " +
                "primary key (ssn))";
    }

    protected static String getTableCountCmd(String table, String keyspace) {
        return "select count(*) from " + keyspace + "." + table;
    }

    protected static String getDropTableCmd(String table, String keyspace) {
        return "drop table if exists " + keyspace + "." + table;
    }

    protected static String getTruncateTableCmd(String table, String keyspace) {
        return "truncate table " + keyspace + "." + table;
    }

    protected static String getCreateTableWithList(String table, String keyspace) {
        return "create table if not exists " + keyspace + "." + table + " (id int, events list<int>, primary key (id));";
    }

    protected static String insertRecordIntoTableCmd(int key, String table) {
        return "insert into " + table + " (id, events) values (" + key + ", []);";
    }

    protected static String updateRecordOfTableCmd(int key, String table) {
        return "update " + table + " SET events=events+[" + incrSeq() + "] where id=" + key + ";";
    }

    // This is only used to fetch the result from the table by session directly connected to cassandra
    protected static String readResultFromTableCmd(int key, String table, String keyspace) {
        return "select events from " + keyspace + "." + table + " where id=" + key + ";";
    }

    protected static long sequencer = 0;

    protected synchronized static long incrSeq() {
        return sequencer++;
    }

    @AfterClass
    public static void teardown() {
        // clean up default table if left
        session.execute(getDropTableCmd(DEFAULT_TABLE_NAME, DEFAULT_KEYSPACE));
        if (client != null) client.close();
        if (singleServer != null) singleServer.close();
        session.close();
        cluster.close();
    }

    protected static Object getInstance(Constructor<?> constructor,
                                      Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected static Constructor<?> getConstructor(String clazz, Class<?>... types) {
        try {
            Class<?> instance = Class.forName(clazz);
            return instance.getConstructor(types);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(GraderSingleServer.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }

}
