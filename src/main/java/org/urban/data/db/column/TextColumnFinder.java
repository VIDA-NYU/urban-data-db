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
 * Create dataset column files.
 * 
 * Parses a directory of dataset files. Transforms each CSV or TSV file in
 * the input directory into a set of files, one for each column in the dataset.
 * Considers any file with suffix .csv, .csv.gz, .tsv, or .tsv.gz as input
 * dataset files.
 * 
 * Assumes that the first row of each dataset file contains the column names.
 * 
 * Output files are numbered 0 to n. The file number corresponds to the unique
 * column identifier that will be used by other programs in the D6 workflow.
 * 
 * Information about individual columns is written to an output file. The file
 * is in JSON format and contains one record per database column.
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
        new TermIndexReader().read(termFile, filter);
        
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
