package org.ut.biolab;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Thread executing a query on a shard. The value we want to pass back has to be
 * an integer for now.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 * 
 */
public class QueryExecutor implements Callable<Integer> {
    // TODO: generalize to other types if needed (cannot use ResultSet due to
    // connection closing or generics due to arbitrary ResultSet structure)
    private int shard = 0;
    private String query = "";
    private QueryTimer qt = null;
    private static Connection conn = null;

    public QueryExecutor(int shard, String query) {
        this.shard = shard;
        this.query = query;
        qt = new QueryTimer();
    }

    /**
     * Obtains a connection to the database and executes a query.
     * 
     * @return
     * 
     */
    public Integer call() {
        System.out.println("Querying started: shard " + shard);
        connect();
        qt.start();

        Integer res = runQuery(query);

        qt.stop();
        disconnect();
        System.out.println("Querying finished - shard, duration (s): " + shard + ", " + qt.getDurationInS());

        return res;
    }

    private int runQuery(String q) {
        int res = 0;
        PreparedStatement s = null;
        try {
            s = conn.prepareStatement(q);
        } catch (SQLException e) {
            System.err.println("Failed to create query.");
        }

        ResultSet r = null;
        try {
            r = s.executeQuery();
            // while (r.next()) {
            // res += r.getInt(1);
            // }

            // just return 1 if the query succeeded, we only want to measure
            // query execution time
            res = 1;
        } catch (SQLException e) {
            System.err.println("Failed to execute query.");
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException e) {
                    System.err.println("Failed to close the statement.");
                }
            }
            if (r != null) {
                try {
                    r.close();
                } catch (SQLException e) {
                    System.err.println("Resultset could not be closed.");
                }
            }
        }

        return res;
    }

    private void connect() {
        conn = ConnectionManager.getInstance().getConnection();
    }

    private void disconnect() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection.");
        }
    }
}
