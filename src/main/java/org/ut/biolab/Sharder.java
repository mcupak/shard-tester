package org.ut.biolab;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Main sharding class.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 */
public class Sharder {

    public static final String DEFAULT_CONFIG_FILE = "config.properties";
    private static Properties config = new Properties();
    private static Connection conn = null;
    private static int shardCount = 0;
    private static ShardManager shardManager;

    private static void loadProperties(Properties prop, String file) {
        try {
            prop.load(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            System.err.println("File " + file + " not found.");
        } catch (IOException e) {
            System.err.println("File " + file + " could not be read.");
        }
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
    public static Connection connect(String host, Integer port, String database, String user, String password) {
        Connection c = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            c = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database, user, password);
        } catch (SQLException e) {
            System.err.println("Could not connect to the database.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("Could not connect to the database.");
            e.printStackTrace();
        }

        return c;
    }

    /**
     * Disconnects from the database.
     * 
     * @param c
     */
    public static void disconnect(Connection c) {
        try {
            c.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection.");
        }
    }

    private static void testTable(Connection c, String table) {
        // run simple select to test the connection and table
        PreparedStatement testSelect = null;
        try {
            testSelect = c.prepareStatement("SELECT * FROM " + table + " LIMIT 2");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ResultSet results;
        try {
            results = testSelect.executeQuery();
            while (results.next()) {
                System.out.println(results.getInt(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Controls the execution.
     * 
     * @param args
     */
    public static void main(String[] args) {
        shardManager = ShardManager.getInstance();
        
        // load properties from file
        loadProperties(config, DEFAULT_CONFIG_FILE);

        // parse command line args
        // TODO: add the posibility to override properties from file via command
        // line args (via gnu getopt)

        // connect to DB
        String host = config.getProperty("dbhost");
        Integer port = Integer.valueOf(config.getProperty("dbport"));
        String database = config.getProperty("dbname");
        String user = config.getProperty("dbuser");
        String password = config.getProperty("dbpassword");
        conn = connect(host, port, database, user, password);

        // create shards
        String table = config.getProperty("dbtable");
        shardCount = Integer.valueOf(config.getProperty("shardno"));
        String file = "/tmp/buffer.tmp";
        if (shardCount > 1) {
            // create separate tables as shards
            // ShardManager.cleanUp(conn, table, shardCount);
            shardManager.createShards(conn, table, shardCount);
            shardManager.fillShardsViaFile(conn, table, shardCount, file);
        } else {
            // use the original table
        }
        
        

        // disconnect
        shardManager.cleanUp(conn, table, shardCount);
        disconnect(conn);
    }
}
