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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.value.profiling.types.DefaultDataTypeAnnotator;
import org.urban.data.core.value.profiling.types.DataTypeLabel;
import org.urban.data.core.util.count.Counter;
import org.urban.data.core.value.DefaultValueTransformer;
import org.urban.data.core.value.LengthAndTokenCountFilter;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueFilter;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.io.ColumnReader;
import org.urban.data.db.io.ValueColumnsReaderFactory;

/**
 * Create a term index file. The output file is tab-delimited and contains three
 * columns: (1) the term identifier, (2) the term, and a comma-separated list of
 * column identifier:count pairs.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexGenerator {

    private final static String COMMAND =
	    "Usage:\n" +
	    "  <input-directory>\n" +
            "  <max-value-length> [-1 to ignore]\n" +
            "  <max-token-count> [-1 to ignore]\n" +
	    "  <mem-buffer-size>\n" +
	    "  <output-file>";
	    
    private static final Logger LOGGER = Logger.getLogger(TermIndexGenerator.class.getName());
 
    private class IOTerm {

        private final HashObjectSet<IdentifiableCount> _columns;
        private final String _term;

        public IOTerm(String term, HashObjectSet<IdentifiableCount> columns) {

            _term = term;
            _columns = columns;
        }

        public HashObjectSet<IdentifiableCount> columns() {

            return _columns;
        }
        
        public IOTerm merge(IOTerm t) {
            
            HashObjectSet<IdentifiableCount> columns = new HashObjectSet<>(_columns);
            
            for (IdentifiableCount col : t.columns()) {
                int id = col.id();
                if (columns.contains(id)) {
                    int value = columns.get(id).count() + col.count();
                    columns.put(new IdentifiableCount(id, value));
                } else {
                    columns.add(col);
                }
            }
            
            return new IOTerm(_term, columns);
        }

        public String term() {

            return _term;
        }

        public void write(PrintWriter out) {

            out.println(
                    this.term() + "\t" +
                    ColumnElementHelper.toArrayString(_columns.toList())
            );
        }
    }

    private class TermFileMerger {
    
        public int merge(TermSetIterator reader1, TermSetIterator reader2, OutputStream os) throws java.io.IOException {

            int lineCount = 0;
            
            try (PrintWriter out = new PrintWriter(os)) {
                while ((!reader1.done()) && (!reader2.done())) {
                    IOTerm t1 = reader1.term();
                    IOTerm t2 = reader2.term();
                    int comp = t1.term().compareTo(t2.term());
                    if (comp < 0) {
                        t1.write(out);
                        reader1.next();
                    } else if (comp > 0) {
                        t2.write(out);
                        reader2.next();
                    } else {
                        t1.merge(t2).write(out);
                        reader1.next();
                        reader2.next();
                    }
                    lineCount++;
                }
                while (!reader1.done()) {
                    reader1.term().write(out);
                    reader1.next();
                    lineCount++;
                }
                while (!reader2.done()) {
                    reader2.term().write(out);
                    reader2.next();
                    lineCount++;
                }
            }
            return lineCount;
        }
    }

    private interface TermSetIterator {
    
        public boolean done();
        public void next() throws java.io.IOException;
        public IOTerm term();
    }
    
    private class TermFileReader implements TermSetIterator {
    
        private BufferedReader _in = null;
        private IOTerm _term = null;

        public TermFileReader(InputStream is) throws java.io.IOException {

            _in = new BufferedReader(new InputStreamReader(is));

            this.readNext();
        }

        public TermFileReader(File file) throws java.io.IOException {

            this(FileSystem.openFile(file));
        }

        @Override
        public boolean done() {

            return (_term == null);
        }

        @Override
        public void next() throws java.io.IOException {

            if ((_in != null) && (_term != null)) {
                this.readNext();
            }
        }

        private void readNext() throws java.io.IOException {

            String line = _in.readLine();
            if (line != null) {
                String[] tokens = line.split("\t");
		HashObjectSet<IdentifiableCount> columns = new HashObjectSet<>();
                for (String pairString : tokens[1].split(",")) {
                    columns.add(new IdentifiableCount(pairString));
                }
                _term = new IOTerm(tokens[0], columns);
            } else {
                _term = null;
                _in.close();
                _in = null;
            }
        }

        @Override
        public IOTerm term() {

            return _term;
        }
    }
    
    private class TermSetReader implements TermSetIterator {

        private final ArrayList<String> _terms;
        private final HashMap<String, HashObjectSet<IdentifiableCount>> _termIndex;
        private int _readIndex;
        
        public TermSetReader(
                ArrayList<String> terms,
                HashMap<String, HashObjectSet<IdentifiableCount>> termIndex
        ) {
        
            _terms = terms;
            _termIndex = termIndex;
            
            _readIndex = 0;
        }
        
        @Override
        public boolean done() {

            return (_readIndex >= _terms.size());
        }

        @Override
        public void next() {
            
            _readIndex++;
        }

        @Override
        public IOTerm term() {

            String term = _terms.get(_readIndex);
            return new IOTerm(term, _termIndex.get(term));
        }
    }

    public void createIndex(
            ValueColumnsReaderFactory readers,
            ValueFilter filter,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        DefaultValueTransformer transformer = new DefaultValueTransformer();
        
        HashMap<String, HashObjectSet<IdentifiableCount>> termIndex = new HashMap<>();
        while (readers.hasNext()) {
            ColumnReader reader = readers.next();
            HashMap<String, Counter> termCounts = new HashMap<>();
            while (reader.hasNext()) {
                ValueCounter value = reader.next();
                String term = transformer.transform(value.getText());
                if (filter.accept(term)) {
                    if (!termCounts.containsKey(term)) {
                         termCounts.put(term, new Counter(value.getCount()));
                   } else {
                        termCounts.get(term).inc(value.getCount());
                    }
                }
            }
            for (String term : termCounts.keySet()) {
                IdentifiableCount colCount = new IdentifiableCount(
                        reader.columnId(),
                        termCounts.get(term).value()
                );
                if (!termIndex.containsKey(term)) {
                    HashObjectSet<IdentifiableCount> columns = new HashObjectSet<>();
                    columns.add(colCount);
                    termIndex.put(term, columns);
                } else {
                    HashObjectSet<IdentifiableCount> columns = termIndex.get(term);
                    if (columns.contains(colCount.id())) {
                        int value = columns.get(colCount.id()).count() + colCount.count();
                        columns.add(new IdentifiableCount(colCount.id(), value));
                    } else {
                        columns.add(colCount);
                    }
                }
            }
            if (termIndex.size() > bufferSize) {
                writeTermIndex(termIndex, outputFile);
                termIndex = new HashMap<>();
            }
        }
        if (!termIndex.isEmpty()) {
            writeTermIndex(termIndex, outputFile);
        }
        // Add unique term id and term data type information to terms in
        // current output file.
        DefaultDataTypeAnnotator typeCheck = new DefaultDataTypeAnnotator();
        File tmpFile = File.createTempFile("tmp", outputFile.getName());
        try (
                BufferedReader in = FileSystem.openReader(outputFile);
                PrintWriter out = FileSystem.openPrintWriter(tmpFile)
        ) {
            String line;
            int termId = 0;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                String term = tokens[0];
                DataTypeLabel type = typeCheck.getType(term);
                out.println(termId + "\t" + term + "\t" + type.id() + "\t" + tokens[1]);
                termId++;
            }
        }
        FileSystem.copy(tmpFile, outputFile);
        Files.delete(tmpFile.toPath());
    }
    
    public void run(
            File inputDir,
            int maxValueLength,
            int maxTokenCount,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        
        // Create the directory for the output file if it does not exist.
        if (!outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        } else if (outputFile.exists()) {
            outputFile.delete();
        }
        
        this.createIndex(
                new ValueColumnsReaderFactory(inputDir),
                new LengthAndTokenCountFilter(maxValueLength, maxTokenCount),
                bufferSize,
                outputFile
        );
    }

    public void run(
            File inputDir,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        this.run(inputDir, -1, -1, bufferSize, outputFile);
    }
    
    private void writeTermIndex(
            HashMap<String, HashObjectSet<IdentifiableCount>> termIndex,
            File outputFile
    ) throws java.io.IOException {

        ArrayList<String> terms = new ArrayList<>(termIndex.keySet());
	Collections.sort(terms);
	
        if (!outputFile.exists()) {
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String term : terms) {
                    out.println(
                            term + "\t" +
                            ColumnElementHelper.toArrayString(
                                    termIndex.get(term).toList()
                            )
                    );
                }
            }
            System.out.println("INITIAL FILE HAS " + terms.size() + " ROWS.");
        } else {
            System.out.println("MERGE " + terms.size() + " TERMS.");
            File tmpFile = File.createTempFile("tmp", outputFile.getName());
            int count = new TermFileMerger().merge(
                    new TermFileReader(outputFile),
                    new TermSetReader(terms, termIndex),
                    FileSystem.openOutputFile(tmpFile)
            );
            Files.copy(
                    tmpFile.toPath(),
                    outputFile.toPath(),
                    new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
            );
            Files.delete(tmpFile.toPath());
            System.out.println("MERGED FILE HAS " + count + " ROWS.");
        }
    }
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        int maxValueLength = Integer.parseInt(args[1]);
        int maxTokenCount = Integer.parseInt(args[2]);
        int bufferSize = Integer.parseInt(args[3]);
        File outputFile = new File(args[4]);
        
        try {
            new TermIndexGenerator().run(
                    inputDirectory,
                    maxValueLength,
                    maxTokenCount,
                    bufferSize,
                    outputFile
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
