package org.ut.biolab;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class for controlling shards.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 * 
 */
public class ShardManager {
    private static ShardManager instance = null;

    protected ShardManager() {
        // exists only to defeat instantiation.
    }

    public static ShardManager getInstance() {
        if (instance == null) {
            instance = new ShardManager();
        }
        return instance;
    }

    /**
     * Generate a table name for a given shard.
     * 
     * @param table
     * @param index
     * @return
     */
    private String getShardName(String table, int index) {
        return table + "_" + "s" + index;
    }

    /**
     * Creates tables for shards based on the schema of the parent table.
     * 
     * @param c
     * @param table
     * @param shards
     */
    public void createShards(Connection c, String table, int shards) {
        PreparedStatement p;
        for (int i = 0; i < shards; i++) {
            // create table for a shard
            try {
                p = c.prepareStatement("CREATE TABLE IF NOT EXISTS " + getShardName(table, i) + " LIKE " + table);
                System.out.println("Creating shard " + i);
                p.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Determines the number of records in a table.
     * 
     * @param c
     * @param table
     * @return
     */
    public int countRecords(Connection c, String table) {
        int count = 1;
        PreparedStatement s = null;
        try {
            s = c.prepareStatement("SELECT count(*) FROM " + table);
            ResultSet co = s.executeQuery();
            co.next();
            count = co.getInt(1);
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return count;
    }

    /**
     * Fill shards with data from the original table.
     * 
     * @param c
     * @param table
     * @param shards
     */
    public void fillShards(Connection c, String table, int shards) {
        // do everything in a transaction to ensure consistency
        PreparedStatement p = null;
        try {
            c.setAutoCommit(false);
            int piece = countRecords(c, table);
            c.commit();
            for (int i = 0; i < shards; i++) {
                // distribute the original table's data to shards round-robin
                // (different strategy is also possible)
                p = c.prepareStatement("INSERT INTO " + getShardName(table, i) + " SELECT * FROM " + table + " LIMIT " + piece + " OFFSET " + piece * i);
                System.out.println("Creating shard " + i);
                p.execute();
            }
            c.commit();
            c.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (p != null) {
                try {
                    p.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Fill shards with data from the original table. If your storage does not
     * support direct insert (like ICE), use this instead of fillShards.
     * 
     * Be careful with this, ideally run with database running on localhost as
     * it can transfer huge amounts of data.
     * 
     * Currently recycles a single file and performs operations in a serial
     * manner. There is potential speedup if this part is parallellized,
     * especially if you are testing on a machine with multiple hard disks.
     * Howver, since sharding is usually only performed once and we are only
     * interested in the time it takes to run the queries, we do not mind if
     * this operation takes slightly more time than optimum.
     * 
     * @param c
     * @param table
     * @param shards
     * @param buffer
     */
    public void fillShardsViaFile(Connection c, String table, int shards, String file) {
        PreparedStatement p = null;
        File f = new File(file);
        try {
            int piece = (countRecords(c, table) + 1) / shards;
            for (int i = 0; i < shards; i++) {
                // distribute the original table's data to shards round-robin
                // (different strategy is also possible)
                System.out.println("Filling in shard " + i);
                p = c.prepareStatement("SELECT * FROM " + table + " LIMIT " + piece + " OFFSET " + piece * i + " INTO OUTFILE '" + file + "' fields terminated by '\t'");
                p.execute();
                p = c.prepareStatement("LOAD DATA INFILE '" + file + "' INTO TABLE " + getShardName(table, i) + " fields terminated by '\t'");
                p.execute();

                // remove temp file
                if (f.exists())
                    f.delete();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (p != null) {
                try {
                    p.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Gets rid of the tables.
     * 
     * @param c
     * @param table
     * @param shards
     */
    public void cleanUp(Connection c, String table, int shards) {
        if (shards > 1) {
            PreparedStatement p = null;
            for (int i = 0; i < shards; i++) {
                // drop table
                try {
                    p = c.prepareStatement("DROP TABLE IF EXISTS " + getShardName(table, i));
                    System.out.println("Deleting shard " + i);
                    p.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (p != null) {
                try {
                    p.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
