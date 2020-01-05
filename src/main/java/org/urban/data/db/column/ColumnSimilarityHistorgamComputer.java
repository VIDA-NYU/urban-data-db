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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.similarity.ParallelSetSimilarityComputer;
import org.urban.data.core.set.similarity.SetSimilarityComputer;
import org.urban.data.core.similarity.JaccardIndex;
import org.urban.data.core.util.SimilarityHistogram;
import org.urban.data.db.eq.EQIndex;

/**
 * Compute pairwise similarity between all columns in a database.
 * 
 * Expects a term-index file as input.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnSimilarityHistorgamComputer {
   
    /**
     * Similarity function for database columns. Similarity is computed as
     * Jaccard Similarity between the column term sets.
     */
    private class JIColumnSimilarity implements SetSimilarityComputer<Column> {

        private final int[] _nodeSizes;
        
        public JIColumnSimilarity(EQIndex eqIndex) {
            
            _nodeSizes = eqIndex.nodeSizes();
        }
        
        @Override
        public BigDecimal getSimilarity(Column col1, Column col2) {

            int setSize1 = 0;
            int setSize2 = 0;
            int overlap = 0;
            
            for (int nodeId : col1) {
                final int termCount =  _nodeSizes[nodeId];
                if (col2.contains(nodeId)) {
                    overlap += termCount;
                }
                setSize1 += termCount;
            }
            for (int nodeId : col2) {
                setSize2 += _nodeSizes[nodeId];
            }
            
            if (overlap > 0) {
                return JaccardIndex.ji(setSize1, setSize2, overlap);
            } else {
                return BigDecimal.ZERO;
            }
        }

    }
    
    public void run(
            EQIndex eqIndex,
            int threads,
            PrintWriter out
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        SimilarityHistogram histogram = new SimilarityHistogram();
        
        new ParallelSetSimilarityComputer<Column>().run(
                eqIndex.columns().toList(),
                new JIColumnSimilarity(eqIndex),
                threads,
                histogram
        );
        
        histogram.write(out);
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>\n" +
            "  {<output-file>}";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnSimilarityHistorgamComputer.class.getName());
    
    public static void main(String[] args) {
        
        if ((args.length < 2) || (args.length > 3)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputFile = null;
        if (args.length == 3) {
            outputFile = new File(args[2]);
        }

        
        try {
            PrintWriter out;
            if (outputFile != null) {
                out = FileSystem.openPrintWriter(outputFile);
            } else {
                out = new PrintWriter(System.out);
            }
            new ColumnSimilarityHistorgamComputer()
                    .run(new EQIndex(eqFile), threads, out);
            out.close();
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
