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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.util.count.Counter;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.StringSet;
import org.urban.data.core.value.ngram.NGramGenerator;
import org.urban.data.db.io.ColumnReader;
import org.urban.data.db.io.ColumnReaderFactory;
import org.urban.data.db.io.ValueColumnsReaderFactory;

/**
 * Convert a set of database columns into n-gram columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NGramColumnGenerator {

    private final static String COMMAND =
	    "Usage:\n" +
	    "  <input-directory>\n" +
            "  <ngram-size>\n" +
            "  <pad-for-short-values> [true | false]\n" +
	    "  <output-directory>";
	    
    private static final Logger LOGGER = Logger.getGlobal();
 
    public void run(
            File inputDir,
            int q,
            boolean padShortValues,
            File outputDir
    ) throws java.io.IOException {

        ColumnReaderFactory readers = new ValueColumnsReaderFactory(inputDir);
        
        NGramGenerator nGramGen = new NGramGenerator(q);
        
        while (readers.hasNext()) {
            ColumnReader reader = readers.next();
            HashMap<String, Counter> nGrams = new HashMap<>();
            while (reader.hasNext()) {
                ValueCounter value = reader.next();
                StringSet grams;
                if (padShortValues) {
                    grams = nGramGen.getPaddedNGrams(value.getText());
                } else {
                    grams = nGramGen.getNGrams(value.getText());
                }
                for (String ngram : grams) {
                    if (!nGrams.containsKey(ngram)) {
                         nGrams.put(ngram, new Counter(value.getCount()));
                    } else {
                        nGrams.get(ngram).inc(value.getCount());
                    }
                }
            }
            List<String> sortedNGramd = new ArrayList<>(nGrams.keySet());
            Collections.sort(sortedNGramd);
            File outputFile = new File(
                    outputDir.getAbsolutePath() +
                    File.separator +
                    reader.columnId() + ".txt.gz"
            );
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String ngram : sortedNGramd) {
                    out.println(ngram + "\t" + nGrams.get(ngram).value());
                }
            }
        }
    }
        
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        int q = Integer.parseInt(args[1]);
        boolean padShortValues = Boolean.parseBoolean(args[2]);
        File outputFile = new File(args[3]);
        
        try {
            new NGramColumnGenerator().run(
                    inputDirectory,
                    q,
                    padShortValues,
                    outputFile
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
