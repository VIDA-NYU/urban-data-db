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
import org.urban.data.core.set.HashIDSet;
import org.urban.data.db.term.TermColumnProjection;
import org.urban.data.db.term.TermIndexReader;

/**
 * Generate compressed equivalence class files for text columns in all domains
 * in the data study.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainsTextEQsGenerator {
       
    public void run(File baseDir) throws java.io.IOException {
        
        for (File domainDir : baseDir.listFiles()) {
            if (!domainDir.isDirectory()) {
                continue;
            }
            File textColumnsFile = FileSystem.joinPath(
                    domainDir,
                    "text-columns.txt"
            );
            if (!textColumnsFile.exists()) {
                continue;
            }
            File eqIndexFile = FileSystem.joinPath(domainDir, "compressed-term-index.TEXT.txt.gz");
            String domain = domainDir.getName();
            File termIndexFile = FileSystem.joinPath(domainDir, "term-index.txt.gz");
            try (PrintWriter out = FileSystem.openPrintWriter(eqIndexFile)) {
                TermColumnProjection consumer = new TermColumnProjection(
                        new HashIDSet(textColumnsFile),
                        new CompressedTermIndexGenerator(out, domain, 1000)
                );
                new TermIndexReader(termIndexFile).read(consumer);
            }
        }
    }

    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DomainsTextEQsGenerator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Domains Text EQ File Generator - 0.1.4");
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);

        try {
            new DomainsTextEQsGenerator().run(baseDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
