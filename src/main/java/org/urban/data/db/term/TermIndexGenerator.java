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
import org.urban.data.core.constraint.ThresholdConstraint;
import org.urban.data.core.value.profiling.types.DefaultDataTypeAnnotator;
import org.urban.data.core.value.profiling.types.DataTypeLabel;
import org.urban.data.core.value.DefaultValueTransformer;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.MemUsagePrinter;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.profiling.types.ValueTypeFactory;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.column.ColumnTypeConstraint;
import org.urban.data.db.column.NoneTypeConstraint;
import org.urban.data.db.column.TextColumnConstraint;
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
            "  <column-filter> [NONE | TEXT:<constraint>]\n" +
	    "  <mem-buffer-size>\n" +
	    "  <output-file>";
    
    private class ColumnValue {
        
        private int _count;
        private final DataTypeLabel _type;
        
        public ColumnValue(DataTypeLabel type, int count) {
            
            _type = type;
            _count = count;
        }
        
        public int count() {
            
            return _count;
        }
        
        public void inc(int value) {
            
            _count += value;
        }
        
        public DataTypeLabel type() {
            
            return _type;
        }
    }
    
    private class IndexValue {
        
        private final HashObjectSet<IdentifiableCount> _counts;
        private final DataTypeLabel _type;
        
        public IndexValue(DataTypeLabel type, IdentifiableCount count) {
            
            _type = type;
            
            _counts = new HashObjectSet<>();
            _counts.add(count);
        }
        
        public void add(IdentifiableCount column) {
        
            if (_counts.contains(column.id())) {
                throw new IllegalArgumentException("Count for column " + column.id() + " exists");
            }
            _counts.add(column);
        }
        
        public HashObjectSet<IdentifiableCount> counts() {
            
            return _counts;
        }
        
        
        public DataTypeLabel type() {
            
            return _type;
        }
    }
    
    private class IOTerm {

        private final HashObjectSet<IdentifiableCount> _columns;
        private final String _term;
        private final DataTypeLabel _type;

        public IOTerm(
                String term,
                DataTypeLabel type,
                HashObjectSet<IdentifiableCount> columns
        ) {
            _term = term;
            _type = type;
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
            
            return new IOTerm(_term, _type, columns);
        }

        public String term() {

            return _term;
        }

        public void write(PrintWriter out) {

            out.println(
                    this.term() + "\t" +
                    _type.id() + "\t" +
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
        private final ValueTypeFactory _typeFactory = new ValueTypeFactory();
        
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
                for (String pairString : tokens[2].split(",")) {
                    columns.add(new IdentifiableCount(pairString));
                }
                _term = new IOTerm(
                        tokens[0],
                        _typeFactory.get(Integer.parseInt(tokens[1])),
                        columns
                );
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
        private final HashMap<String, IndexValue> _termIndex;
        private int _readIndex;
        
        public TermSetReader(
                ArrayList<String> terms,
                HashMap<String, IndexValue> termIndex
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
            IndexValue entry = _termIndex.get(term);
            return new IOTerm(term, entry.type(), entry.counts());
        }
    }

    public void createIndex(
            ValueColumnsReaderFactory readers,
            ColumnTypeConstraint constraint,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        DefaultValueTransformer transformer = new DefaultValueTransformer();
        DefaultDataTypeAnnotator typeCheck = new DefaultDataTypeAnnotator();
        
        HashMap<String, IndexValue> termIndex = new HashMap<>();
        while (readers.hasNext()) {
            ColumnReader reader = readers.next();
            HashMap<String, ColumnValue> columnValues = new HashMap<>();
            constraint.reset();
            while (reader.hasNext()) {
                ValueCounter colVal = reader.next();
                String term = transformer.transform(colVal.getText());
                if (!columnValues.containsKey(term)) {
                    DataTypeLabel type = typeCheck.getType(term);
                    columnValues.put(term, new ColumnValue(type, colVal.getCount()));
                    constraint.consume(type);
                } else {
                    columnValues.get(term).inc(colVal.getCount());
                }
            }
            if (constraint.isSatisfied()) {
                for (String term : columnValues.keySet()) {
                    ColumnValue value = columnValues.get(term);
                    IdentifiableCount colCount = new IdentifiableCount(
                            reader.columnId(),
                            value.count()
                    );
                    if (!termIndex.containsKey(term)) {
                        HashObjectSet<IdentifiableCount> columns = new HashObjectSet<>();
                        columns.add(colCount);
                        termIndex.put(term, new IndexValue(value.type(), colCount));
                    } else {
                        termIndex.get(term).add(colCount);
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
        File tmpFile = File.createTempFile("tmp", outputFile.getName());
        try (
                BufferedReader in = FileSystem.openReader(outputFile);
                PrintWriter out = FileSystem.openPrintWriter(tmpFile)
        ) {
            String line;
            int termId = 0;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                out.println(termId + "\t" + tokens[0] + "\t" + tokens[1] + "\t" + tokens[2]);
                termId++;
            }
        }
        FileSystem.copy(tmpFile, outputFile);
        Files.delete(tmpFile.toPath());
    }
    
    public void run(
            File inputDir,
            ColumnTypeConstraint columnConstraint,
            int bufferSize,
            File outputFile
    ) throws java.io.IOException {
        
        
        // Create the directory for the output file if it does not exist.
        FileSystem.createParentFolder(outputFile);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        
        this.createIndex(
                new ValueColumnsReaderFactory(inputDir),
                columnConstraint,
                bufferSize,
                outputFile
        );
    }

    private void writeTermIndex(
            HashMap<String, IndexValue> termIndex,
            File outputFile
    ) throws java.io.IOException {

        ArrayList<String> terms = new ArrayList<>(termIndex.keySet());
	Collections.sort(terms);
	
        if (!outputFile.exists()) {
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String term : terms) {
                    IndexValue entry = termIndex.get(term);
                    out.println(
                            term + "\t" +
                            entry.type().id() + "\t" +
                            ColumnElementHelper.toArrayString(
                                    entry.counts().toList()
                            )
                    );
                }
            }
            System.out.println("INITIAL FILE HAS " + termIndex.size() + " ROWS.");
        } else {
            System.out.println("MERGE " + termIndex.size() + " TERMS.");
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
        
        new MemUsagePrinter().print("MEMORY USAGE");
    }
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File inputDirectory = new File(args[0]);
        String columnConstraint = args[1].toUpperCase();
        int bufferSize = Integer.parseInt(args[2]);
        File outputFile = new File(args[3]);
        
        ColumnTypeConstraint constraint = null;
        if (columnConstraint.equals("NONE")) {
            constraint = new NoneTypeConstraint();
        } else if (columnConstraint.startsWith("TEXT:")) {
            constraint = new TextColumnConstraint(
                    ThresholdConstraint.getConstraint(columnConstraint.substring(5))
            );
        } else {
            System.out.println("Invalid column type constraint " + columnConstraint);
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        try {
            new TermIndexGenerator().run(
                    inputDirectory,
                    constraint,
                    bufferSize,
                    outputFile
            );
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "CREATE TERM INDEX", ex);
            System.exit(-1);
        }
    }
}
