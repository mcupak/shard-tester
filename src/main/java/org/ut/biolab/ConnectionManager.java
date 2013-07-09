package org.ut.biolab;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Database connection manager. Connection pool is needed as each thread has to
 * have a separate connection to the database to ensure effectiveness.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 * 
 */
public class ConnectionManager {
    private static ConnectionManager instance = null;
    private ComboPooledDataSource cpds = null;

    protected ConnectionManager() {
        // exists only to defeat instantiation.
    }

    public static ConnectionManager getInstance() {
        if (instance == null) {
            instance = new ConnectionManager();
        }
        return instance;
    }

    /**
     * Initialized the datasource. Call before running queries.
     * 
     * @param host
     * @param port
     * @param database
     * @param user
     * @param password
     * @param connectionNo
     */
    public void init(String host, Integer port, String database, String user, String password, int connectionNo) {
        cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            System.err.println("Invalid DB driver.");
        }
        cpds.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        cpds.setUser(user);
        cpds.setPassword(password);

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(3);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(connectionNo);
    }

    /**
     * Retrieves an available connection.
     * 
     * @return
     */
    public Connection getConnection() {
        Connection c = null;
        try {
            c = cpds.getConnection();
        } catch (SQLException e) {
            System.err.println("Database access error.");
        }

        return c;
    }

    /**
     * Obtains connection-related stats.
     * 
     * @return
     */
    public String getStatus() {
        StringBuffer res = new StringBuffer();
        if (cpds != null) {
            try {
                res.append("connections: ");
                res.append(cpds.getNumConnectionsDefaultUser());
                res.append("\nbusy connections: ");
                res.append(cpds.getNumBusyConnectionsDefaultUser());
                res.append("\nidle connections: ");
                res.append(cpds.getNumIdleConnectionsDefaultUser());
            } catch (SQLException e) {
                System.err.println("Unable to obtain stats.");
            }
        }
        return res.toString();
    }
}
