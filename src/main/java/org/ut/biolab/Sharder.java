package org.ut.biolab;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Main sharding class.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 */
public class Sharder {

    public static final String DEFAULT_CONFIG_FILE = "config.properties";
    public static final String SELECT_STAR_TEMPLATE = "SELECT variant_id FROM %s";
    public static final String COUNT_STAR_TEMPLATE = "SELECT COUNT(*) FROM %s";
    public static final String SINGLE_MATCH_WHERE_TEMPLATE = "SELECT * FROM %s WHERE variant_id=200000";
    public static final String INTERVAL_TEMPLATE = "SELECT * FROM %s WHERE variant_id BETWEEN 500000 AND 600000";
    public static final String PATTERN_TEMPLATE = "SELECT * FROM %s WHERE ref LIKE '%%GGG%%'";

    private static Properties config = new Properties();
    private static int shardCount = 0;
    private static ShardManager sManager = null;
    private static QueryExecutorManager qeManager = null;
    private static ConnectionManager cManager = null;
    private static Connection conn = null;

    private static void loadProperties(Properties prop, String file) throws FileNotFoundException, IOException {
        prop.load(new FileInputStream(file));
    }

    /**
     * Connects to the database.
     * 
     * @param host
     * @param port
     * @param database
     * @param user
     * @param password
     * @return
     */
    public static void connect(String host, Integer port, String database, String user, String password, int connNo) {
        cManager = ConnectionManager.getInstance();
        cManager.init(host, port, database, user, password, connNo);
        conn = cManager.getConnection();
    }

    /**
     * Disconnects from the database.
     * 
     * @param c
     */
    public static void disconnect() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection.");
        }
    }

    /**
     * Controls the execution.
     * 
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("STARTING");
        sManager = ShardManager.getInstance();

        // load properties from file
        try {
            loadProperties(config, DEFAULT_CONFIG_FILE);

            // parse command line args
            // TODO: add the posibility to override properties from file via
            // command line args (via gnu getopt)

            // connect to DB
            String host = config.getProperty("dbhost");
            Integer port = Integer.valueOf(config.getProperty("dbport"));
            String database = config.getProperty("dbname");
            String user = config.getProperty("dbuser");
            String password = config.getProperty("dbpassword");
            shardCount = Integer.valueOf(config.getProperty("shardno"));

            // print config
            System.out.println("Host: " + host + ":" + port);
            System.out.println("DB: " + database);

            // connect
            connect(host, port, database, user, password, shardCount + 1);

            // create shards
            String table = config.getProperty("dbtable");
            String file = config.getProperty("buffer");
            if (shardCount > 0) {
                // create separate tables as shards
                sManager.createShards(conn, table, shardCount);
                sManager.fillShardsViaFile(conn, table, shardCount, file);

                // schedule queries
                qeManager = new QueryExecutorManager(shardCount);
                List<String> queryBuffer = new ArrayList<String>();
                queryBuffer.add(SELECT_STAR_TEMPLATE);
                queryBuffer.add(COUNT_STAR_TEMPLATE);
                queryBuffer.add(SINGLE_MATCH_WHERE_TEMPLATE);
                queryBuffer.add(INTERVAL_TEMPLATE);
                queryBuffer.add(PATTERN_TEMPLATE);

                // run queries
                for (String q : queryBuffer) {
                    System.out.println("Query: " + q);
                    List<Integer> results = qeManager.execute(q, table);

                    // aggregate results and measure the time it takes to merge
                    // trivial merging in this case for the purpose of
                    // comparison
                    QueryTimer qt = new QueryTimer();
                    qt.start();
                    int totalCount = 0;
                    for (Integer r : results) {
                        totalCount += r;
                    }
                    qt.stop();
                    System.out.println("Result, merging time (ms): " + totalCount + ", " + qt.getDurationInMs());
                }
            }

            // disconnect
            sManager.cleanUp(conn, table, shardCount);
            disconnect();
        } catch (FileNotFoundException e) {
            System.err.println("File " + DEFAULT_CONFIG_FILE + " not found.");
        } catch (IOException e) {
            System.err.println("File " + DEFAULT_CONFIG_FILE + " could not be read.");
        }

        System.out.println("FINISHED.");
    }
}
