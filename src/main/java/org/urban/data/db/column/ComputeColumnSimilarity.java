/*
 * Copyright 2018 New York University.
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
package org.urban.data.db.column;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.similarity.ParallelSetSimilarityComputer;
import org.urban.data.core.set.similarity.SetSimilarityComputer;
import org.urban.data.core.similarity.ObjectSimilarityConsumer;
import org.urban.data.core.similarity.SimilarityThresholdFilter;
import org.urban.data.core.similarity.SimilarityWriter;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.column.similarity.ColumnSizeWeightFactory;
import org.urban.data.db.column.similarity.JIColumnSimilarity;
import org.urban.data.db.column.similarity.MaxNormalizedColumnWeightFactory;
import org.urban.data.db.column.similarity.WeightedJIColumnSimilarity;
import org.urban.data.db.io.TermIndexReader;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 * Compute pairwise similarity between all columns in a database.
 * 
 * Expects a term-index file as input.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ComputeColumnSimilarity {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <term-index-file>\n" +
            "  <similarity-function> [ JI | WJI-COLSIZE | WJI-COLMAX ]\n" +
            "  <similarity-threshold>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    /**
     * term consumer to generate database columns from term index.
     * 
     */
    private class Database implements TermConsumer {

        private HashObjectSet<Column> _columns;

        @Override
        public void close() {

        }

        public List<Column> columns() {
            
            return _columns.toList();
        }
        
        @Override
        public void consume(ColumnTerm term) {
        
            for (IdentifiableCount col : term.columns()) {
                Column column;
                if (!_columns.contains(col.id())) {
                    column = new Column(col.id());
                    _columns.add(column);
                } else {
                    column = _columns.get(col.id());
                }
                column.add(term.id());
            }
        }

        @Override
        public void open() {

            _columns = new HashObjectSet<>();
        }
    }
    
    public void run(
            File termIndexFile,
            SetSimilarityComputer<Column> simFunc,
            BigDecimal similarityThreshold,
            int threads,
            ObjectSimilarityConsumer consumer
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        Database db = new Database();
        new TermIndexReader().read(termIndexFile, db);
        
        if (similarityThreshold.compareTo(BigDecimal.ZERO) > 0) {
            consumer = new SimilarityThresholdFilter(
                    consumer,
                    similarityThreshold
            );
        }
        new ParallelSetSimilarityComputer<Column>()
                .run(db.columns(), simFunc, threads, consumer);
    }
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File termIndexFile = new File(args[0]);
        String simFuncName = args[1];
        BigDecimal similarityThreshold = new BigDecimal(args[2])
                .max(BigDecimal.ZERO);
        int threads = Integer.parseInt(args[3]);
        File outputFile = new File(args[4]);
        
        SetSimilarityComputer<Column> simFunc = null;
        if (simFuncName.equalsIgnoreCase("JI")) {
            simFunc = new JIColumnSimilarity();
        } else if (simFuncName.equalsIgnoreCase("WJI-COLSIZE")) {
            try {
                simFunc = new WeightedJIColumnSimilarity(
                        new ColumnSizeWeightFactory().getWeights(termIndexFile)
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "GET COLUMN WEIGHTS", ex);
                System.exit(-1);
            }
        } else if (simFuncName.equalsIgnoreCase("WJI-COLMAX")) {
            try {
                simFunc = new WeightedJIColumnSimilarity(
                        new MaxNormalizedColumnWeightFactory()
                                .getWeights(termIndexFile)
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "GET COLUMN WEIGHTS", ex);
                System.exit(-1);
            }
        } else {
            System.out.println("Unknown similarity function: " + simFuncName);
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ComputeColumnSimilarity()
                    .run(
                            termIndexFile,
                            simFunc,
                            similarityThreshold,
                            threads,
                            new SimilarityWriter(out)
                    );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
