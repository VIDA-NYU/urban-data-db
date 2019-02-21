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
package org.urban.data.db.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 * Filter equivalence classes by the number of columns they occur in.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQClassFilter {

    private static final String COMMAND = 
            "Usage:\n" +
            "  <eq-file>\n" +
	    "  <column-threshold>\n" +
            "  <output-file>";
            
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
	int columnThreshold = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);     
        
        try (
                BufferedReader in = FileSystem.openReader(inputFile);
                PrintWriter out = FileSystem.openPrintWriter(outputFile)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                int colCount = line.split("\t")[2].split(",").length;
                if (colCount >= columnThreshold) {
                    out.println(line);
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, outputFile.getName(), ex);
            System.exit(-1);
        }
    }
}
