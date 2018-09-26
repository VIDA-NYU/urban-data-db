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
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.io.TermIndexReader;

/**
 * Filter terms in a term index that occur in a given set of columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexFilter {

    private final static String COMMAND =
	    "Usage:\n" +
	    "  <input-file>\n" +
            "  <column-list-file>\n" +
	    "  <output-file>";
	    
    private static final Logger LOGGER = Logger.getGlobal();
 
    private class TermColumnFilter implements TermConsumer {

        private final ObjectFilter<Integer> _columnFilter;
        private final PrintWriter _out;
        public TermColumnFilter(
                ObjectFilter<Integer> columnFilter,
                PrintWriter out
        ) {
            _columnFilter = columnFilter;
            _out = out;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(ColumnTerm term) {
            
            HashObjectSet<IdentifiableCount> columns = new HashObjectSet<>();
            for (IdentifiableCount column : term.columns()) {
                if (_columnFilter.contains(column.id())) {
                    columns.add(column);
                }
            }
            if (!columns.isEmpty()) {
                _out.println(
                        term.id() + "\t" +
                        term.value() + "\t" +
                        term.type().id() + "\t" +
                        ColumnElementHelper.toArrayString(columns.toList())
                );
            }
        }

        @Override
        public void open() {

        }
    }
    
    public void run(
            File inputFile,
            ObjectFilter<Integer> columnFilter,
            File outputFile
    ) throws java.io.IOException {
    
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new TermIndexReader()
                    .read(inputFile, new TermColumnFilter(columnFilter, out));
        }
        
    }
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputFile = new File(args[0]);
        File columnListFile = new File(args[1]);
        File outputFile = new File(args[2]);
        
        try {
            new TermIndexFilter().run(
                    inputFile,
                    new HashIDSet(columnListFile),
                    outputFile
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
