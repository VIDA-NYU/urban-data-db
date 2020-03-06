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
package org.urban.data.db.term;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.column.CSVColumnsReaderFactory;

/**
 * Generate column files for all datasets in the study.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainsTermIndexGenerator {
       
    public void run(
            File baseDir,
            List<String> domains,
            int runIndex,
            int step,
            int bufferSize
    ) throws java.io.IOException {
        
        for (int iDomain = runIndex; iDomain < domains.size(); iDomain += step) {
            String domain = domains.get(iDomain);
            File domainDir = FileSystem.joinPath(baseDir, domain);
            if (domainDir.isDirectory()) {
                System.out.println(domain);
                File columnsDir = FileSystem.joinPath(domainDir, "columns");
                File columnsFile = FileSystem.joinPath(domainDir, "columns.tsv");
                if (columnsFile.isFile()) {
                    List<File> files = new ArrayList<>();
                    try (InputStream is = FileSystem.openFile(columnsFile)) {
                        InputStreamReader reader = new InputStreamReader(is);
                        CSVParser parser = new CSVParser(reader, CSVFormat.TDF);
                        for (CSVRecord row : parser) {
                            if (row.size() == 6) {
                                int columnId = Integer.parseInt(row.get(0));
                                String filename = columnId + ".txt.gz";
                                File columnFile = FileSystem
                                        .joinPath(columnsDir, filename);
                                if (columnFile.isFile()) {
                                    files.add(columnFile);
                                }
                            } else {
                                System.out.println(row);
                            }
                        }
                    }
                    File outputFile = FileSystem.joinPath(domainDir, "term-index.txt.gz");
                    CSVColumnsReaderFactory readers;
                    readers = new CSVColumnsReaderFactory(files);
                    new TermIndexGenerator()
                            .createIndex(readers, bufferSize, outputFile);
                }
            }
        }
    }

    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <domain-names-file>\n" +
            "  <buffer-size>\n" +
            "  <run-index>\n" +
            "  <step>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DomainsTermIndexGenerator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Domains Term Index Generator - 0.1.1");
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File domainNamesFile = new File(args[1]);
        int bufferSize = Integer.parseInt(args[2]);
        int runIndex = Integer.parseInt(args[3]);
        int step = Integer.parseInt(args[4]);
        
        List<String> domains = new ArrayList<>();
        try (BufferedReader in = FileSystem.openReader(domainNamesFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                domains.add(line);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "LOAD", ex);
            System.exit(-1);
        }
        
        try {
            new DomainsTermIndexGenerator()
                    .run(baseDir, domains, runIndex, step, bufferSize);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "LOAD", ex);
            System.exit(-1);
        }
    }
}
