/*
 * Copyright 2020 New York University.
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
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.term.TermCounter;
import org.urban.data.db.term.TermIndexReader;

/**
 * Write number of terms and equivalence classes for all domains in the data
 * study.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainsStatsWriter {
       
    public void run(File baseDir, PrintWriter out) throws java.io.IOException {
        
        for (File domainDir : baseDir.listFiles()) {
            if (domainDir.isDirectory()) {
                String domain = domainDir.getName();
                File eqFile = FileSystem.joinPath(domainDir, "compressed-term-index.txt.gz");
                File termFile = FileSystem.joinPath(domainDir, "term-index.txt.gz");
                if ((eqFile.exists()) && (termFile.exists())) {
                    TermCounter termCounter = new TermCounter();
                    new TermIndexReader(termFile).read(termCounter);
                    EQCounter eqCounter = new EQCounter();
                    new EQReader(eqFile).stream(eqCounter);
                    out.println(
                            domain + "\t" +
                            termCounter.termCount() + "\t" +
                            eqCounter.equivalenceClassCount()
                    );
                    System.out.println(
                            domain + "\t" +
                            termCounter.termCount() + "\t" +
                            eqCounter.equivalenceClassCount()
                    );
                }
            }
        }
    }

    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DomainsStatsWriter.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Domain Stats Writer - 0.1.0");
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new DomainsStatsWriter().run(baseDir, out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
