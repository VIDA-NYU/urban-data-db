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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.ThresholdConstraint;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.io.TermIndexReader;
import org.urban.data.db.term.TextColumnFilter;

/**
 * Output list of identifier for columns that are classified as text columns
 * based on the fraction of column values that have been labeled as text and the
 * given text fraction threshold.
 * 
 * The output file contains the column identifier as the only value in each
 * line.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextColumnFinder {
    
    private final static Logger LOGGER = Logger.getLogger(TextColumnFinder.class.getName());
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <term-index-file>\n" +
            "  <text-fraction-threshold>\n" +
            "  <output-file>";

    public void run(
            File termFile,
            ThresholdConstraint threshold,
            File outputFile
    ) throws java.io.IOException {
        
        // Create output directory if it does not exist.
        FileSystem.createParentFolder(outputFile);
        
        TextColumnFilter filter = new TextColumnFilter();
        new TermIndexReader(termFile).read(filter);
        
        int textColumnCount = 0;
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            for (int columnId : filter.columns(threshold)) {
                out.println(columnId);
                textColumnCount++;
            }
        }

        System.out.println("NUMBER OF TEXT COLUMNS : " + textColumnCount);        
    }

    public static void main(String[] args) {
	
	if (args.length != 3) {
	    System.out.println(COMMAND);
	    System.exit(-1);
	}
	
	File termFile = new File(args[0]);
        double textFilterThreshold = Double.parseDouble(args[1]);
	File outputFile =  new File(args[2]);

        try {
            new TextColumnFinder().run(
                    termFile,
                    ThresholdConstraint
                            .getGreaterConstraint(textFilterThreshold),
                    outputFile
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
