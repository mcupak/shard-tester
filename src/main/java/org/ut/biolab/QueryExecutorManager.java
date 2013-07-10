package org.ut.biolab;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Pool controller for threads executing queries.
 * 
 * @author <a href="mailto:mirocupak@gmail.com">Miroslav Cupak</a>
 * 
 */
public class QueryExecutorManager {

    private int queryCount = 0;
    private ExecutorService executor = null;

    public QueryExecutorManager(int queryCount) {
        this.queryCount = queryCount;
    }

    private String instantiateQueryFromTemplate(String template, String param) {
        return String.format(template, param);
    }

    public List<Integer> execute(String query, String table) {
        executor = Executors.newFixedThreadPool(queryCount);
        Set<Future<Integer>> tempResults = new HashSet<Future<Integer>>();

        // spawn threads
        for (int i = 0; i < queryCount; i++) {
            String q = instantiateQueryFromTemplate(query, ShardManager.getShardName(table, i));
            Callable<Integer> worker = new QueryExecutor(i, q);
            Future<Integer> future = executor.submit(worker);
            tempResults.add(future);
        }

        // collect results
        List<Integer> finalResults = new ArrayList<Integer>();
        for (Future<Integer> f : tempResults) {
            try {
                finalResults.add(f.get());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // finish
        executor.shutdown();
        while (!executor.isTerminated()) {
            // wait until everything is done
        }

        return finalResults;
    }
}
