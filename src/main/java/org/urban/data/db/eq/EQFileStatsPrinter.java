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

import org.urban.data.db.io.DefaultEquivalenceClassReader;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Print statistics of an equivalence class file.
 * 
 * Outputs the number of equivalence classes, terms, and non-empty cells in the
 * database.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQFileStatsPrinter {
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <eq-file>";
            
    private static final Logger LOGGER = Logger.getLogger(EQFileStatsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);

	EquivalenceClassStatsConsumer stats = new EquivalenceClassStatsConsumer();
	try {
	    new DefaultEquivalenceClassReader().read(inputFile, stats);
	} catch (java.io.IOException ex) {
	    LOGGER.log(Level.SEVERE, inputFile.getName(), ex);
	    System.exit(-1);
	}
	
	System.out.println("EQUIVALENCE CLASSES: " + stats.getEquivalenceClassCount());
	System.out.println("TERMS              : " + stats.getTermCount());
	System.out.println("CELLS              : " + stats.getCellCount());
    }
}
