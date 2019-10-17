/*
 * Copyright 2019 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.urban.data.db.eq;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.urban.data.core.set.HashObjectSet;

/**
 * Index of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQIndex extends HashObjectSet<EQ> {
   
    private class OverlapComputer implements Runnable {

        private final List<EQ> _nodes;
        private final ConcurrentLinkedQueue<EQ> _queue;
        private final InMemorySimilarityIndex _result;
        
        public OverlapComputer(
                List<EQ> nodes,
                ConcurrentLinkedQueue<EQ> queue,
                InMemorySimilarityIndex result
        ) {
            _nodes = nodes;
            _queue = queue;
            _result = result;
        }
        
        @Override
        public void run() {

            int count = 0;
            EQ nodeI;
            while ((nodeI = _queue.poll()) != null) {
                for (EQ nodeJ : _nodes) {
                    if (nodeI.id() < nodeJ.id()) {
                        int overlap = nodeI.columns().overlap(nodeJ.columns());
                        if (overlap > 0) {
                            _result.add(nodeI.id(), nodeJ.id(), overlap);
                        }
                        count++;
                        if ((count % 1000) == 0) {
                            System.out.println(count + " @ " + new Date());
                        }
                    }
                }
            }
        }
    }
    
    public EQIndex(File eqFile) throws java.io.IOException {
        
        super(new EQReader(eqFile).read());
    }
    
    /**
     * Compute an in-memory index of pairwise overlap between all equivalence
     * classes in the index.
     * 
     * @param threads
     * @return 
     */
    public EQSimilarityIndex computeOverlap(int threads) {
        
        InMemorySimilarityIndex result = new InMemorySimilarityIndex(this);
        
        List<EQ> nodes = this.toList();
        
        ConcurrentLinkedQueue<EQ> queue;
        queue = new ConcurrentLinkedQueue<>(nodes);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            OverlapComputer command = new OverlapComputer(nodes, queue, result);
            es.execute(command);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        return result;
    }
}
