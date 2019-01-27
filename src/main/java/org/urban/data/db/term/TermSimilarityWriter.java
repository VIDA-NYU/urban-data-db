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
package org.urban.data.db.term;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.ThresholdConstraint;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.EntitySet;
import org.urban.data.core.similarity.JaroWinklerStringSimilarity;
import org.urban.data.core.similarity.LevenshteinStringSimilarity;
import org.urban.data.core.similarity.ParallelStringSimilarityComputer;
import org.urban.data.core.similarity.SimilarityWriter;
import org.urban.data.core.similarity.StringSimilarityComputer;

/**
 * Write pairwise similarity between terms to file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermSimilarityWriter {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <term-index-file>\n" +
            "  <distance-measure> [LEVENSHTEIN | JARO-WINKLER]\n" +
            "  <threshold-constraint>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File termFile = new File(args[0]);
        String distanceMeasure = args[1];
        ThresholdConstraint threshold = ThresholdConstraint.getConstraint(args[2]);
        int threads = Integer.parseInt(args[3]);
        File outputFile = new File(args[4]);
        
        StringSimilarityComputer func = null;
        if (distanceMeasure.equalsIgnoreCase("LEVENSHTEIN")) {
            func = new LevenshteinStringSimilarity(threshold);
        } else if (distanceMeasure.equalsIgnoreCase("JARO-WINKLER")) {
            func = new JaroWinklerStringSimilarity(threshold);
        } else {
            System.out.println("Unknown similarity function: " + distanceMeasure);
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        EntitySet terms = null;
        try {
            terms = new EntitySet(termFile);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ TERMS", ex);
            System.exit(-1);
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ParallelStringSimilarityComputer().run(
                    terms.toList(),
                    func,
                    threads,
                    new SimilarityWriter(out)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
