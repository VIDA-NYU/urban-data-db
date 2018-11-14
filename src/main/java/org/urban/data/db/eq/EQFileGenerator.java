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

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.IdentifiableIDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableIdentifiableIDSet;
import org.urban.data.core.util.count.Counter;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.io.TermIndexReader;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;
import org.urban.data.db.term.TermGroupGenerator;

/**
 * Create an equivalence class file from a given term index file.
 * 
 * The column threshold allows to ignore terms that do not occur in many
 * different columns (i.e., ignore all terms that occur in less columns that
 * the given threshold).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQFileGenerator {

    /**
     * Compress a term index into a set of equivalence classes.
     * 
     * The observeFrequencies flag determines if equivalence classes will
     * contain terms that always occur together in the same columns without
     * considering their frequency of occurrence or whether frequencies are
     * taken into account, i.e., the terms occur in the same columns always with
     * the same frequency.
     * 
     */
    private class CompressedTermIndexGenerator implements TermConsumer {

        private final int _columnThreshold;
        private final Counter _counter;
        private HashMap<String, MutableEquivalenceClass> _eqIndex = null;
        private final boolean _observeFrequencies;
        private final PrintWriter _out;

        public CompressedTermIndexGenerator(
                int columnThreshold,
                boolean observeFrequencies,
                HashMap<String, MutableEquivalenceClass> eqIndex,
                PrintWriter out
        ) {
            _columnThreshold = columnThreshold;
            _observeFrequencies = observeFrequencies;
            _eqIndex = eqIndex;
            _out = out;
            
            int maxId = -1;
            for (MutableEquivalenceClass eq : eqIndex.values()) {
                if (eq.id() > maxId) {
                    maxId = eq.id();
                }
            }
            _counter = new Counter(maxId + 1);
        }

        @Override
        public void close() {

            for (MutableEquivalenceClass eq : _eqIndex.values()) {
                eq.write(_out);
            }

            System.out.println("NUMBER OF EQUIVALENCE CLASSES IS " + _eqIndex.size());
        }

        @Override
        public void consume(ColumnTerm term) {

            if (term.columnCount() >= _columnThreshold) {
                IdentifiableObjectSet<IdentifiableCount> columns;
                columns = term.columns();
                String key;
                if (_observeFrequencies) {
                    key = ColumnElementHelper.toArrayString(columns.toList());
                } else {
                    key = columns.keys().toIntString();
                }
                if (_eqIndex.containsKey(key)) {
                    _eqIndex.get(key).add(term);
                } else {
                    HashIDSet terms = new HashIDSet();
                    terms.add(term.id());
                    _eqIndex.put(
                            key,
                            new MutableEquivalenceClass(
                                    _counter.inc(),
                                    terms,
                                    term.columnFrequencies()
                            )
                    );
                }
            }
        }

        @Override
        public void open() {

        }
    }
    
    /**
     * Merge terms that are in the same group into an equivalence class.
     * 
     */
    private class TermGroupMerger implements TermConsumer {

        private HashMap<Integer, MutableEquivalenceClass> _eqIndex = null;
        private final HashObjectSet<IdentifiableIDSet> _groups;
        private final HashMap<Integer, Integer> _termIndex;
        
        public TermGroupMerger(List<IDSet> groups) {
            
            _termIndex = new HashMap<>();
            _groups = new HashObjectSet<>();
            for (IDSet group : groups) {
                int eqId = _groups.length();
                _groups.add(new ImmutableIdentifiableIDSet(eqId, group));
                for (int termId : group) {
                    _termIndex.put(termId, eqId);
                }
            }
        }
        
        @Override
        public void close() {

            System.out.println("GROUPED " + _termIndex.size() + " TERMS INTO " + _eqIndex.size() + " SETS");
        }

        @Override
        public void consume(ColumnTerm term) {

            if (_termIndex.containsKey(term.id())) {
                int eqId = _termIndex.get(term.id());
                if (_eqIndex.containsKey(eqId)) {
                    _eqIndex.get(eqId).add(term);
                } else {
                    HashIDSet terms = new HashIDSet();
                    terms.add(term.id());
                    _eqIndex.put(
                            eqId,
                            new MutableEquivalenceClass(
                                    _eqIndex.size(),
                                    terms,
                                    term.columnFrequencies()
                            )
                    );
                }
            }
        }

        public List<MutableEquivalenceClass> equivalenceClasses() {
            
            return new ArrayList<>(_eqIndex.values());
        }
        
        @Override
        public void open() {

            _eqIndex = new HashMap<>();
        }
    }
    
    public void run(
            File inputFile,
            int columnThreshold,
            boolean observeFrequencies,
            boolean mergeTerms,
            File outputFile
    ) throws java.io.IOException {
        
        // If the merge terms flag is true we first create a grouping of terms
        // based on identity with respect to the letters and digits in their
        // value
        HashMap<String, MutableEquivalenceClass> mergedTerms = new HashMap<>();
        if (mergeTerms) {
            List<IDSet> groups = new TermGroupGenerator().run(inputFile);
            if (!groups.isEmpty()) {
                // Merge grouped terms into the initial set of equivalence
                // classes
                TermGroupMerger merger = new TermGroupMerger(groups);
                new TermIndexReader().read(inputFile, merger);
                for (MutableEquivalenceClass eq : merger.equivalenceClasses()) {
                    // Filter equivalence classes that do not satisfy the column
                    // threshold constraint
                    if (eq.columnCount() >= columnThreshold) {
                        IdentifiableObjectSet<IdentifiableCount> columns;
                        columns = eq.columns();
                        String key;
                        if (observeFrequencies) {
                            key = ColumnElementHelper.toArrayString(columns.toList());
                        } else {
                            key = columns.keys().toIntString();
                        }
                        if (mergedTerms.containsKey(key)) {
                            mergedTerms.get(key).add(eq);
                        } else {
                            mergedTerms.put(key, eq);
                        }
                    }
                }
                System.out.println("INITIAL SET OF " + mergedTerms.size() + " EQUIVALENCE CLASSES");
            }
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
	   CompressedTermIndexGenerator consumer;
	   consumer =  new CompressedTermIndexGenerator(
                   columnThreshold,
                   observeFrequencies,
                   mergedTerms,
                   out
           );
           new TermIndexReader().read(inputFile, consumer);
        }
    }

    private static final String COMMAND = 
            "Usage:\n" +
            "  <term-index-file>\n" +
	    "  <column-threshold>\n" +
            "  <observe-frequencies> [true | false]\n" +
            "  <merge-terms> [true | false]\n" +
            "  <output-file>";
            
    private static final Logger LOGGER = Logger.getLogger(EQFileGenerator.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
	int columnThreshold = Integer.parseInt(args[1]);
        boolean observeFrequencies = Boolean.parseBoolean(args[2]);
        boolean mergeTerms = Boolean.parseBoolean(args[3]);
        File outputFile = new File(args[3]);     
        
        try {
            new EQFileGenerator().run(
                    inputFile,
                    columnThreshold,
                    observeFrequencies,
                    mergeTerms,
                    outputFile
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, outputFile.getName(), ex);
            System.exit(-1);
        }
    }
}
