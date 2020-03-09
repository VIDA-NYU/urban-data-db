/*
 * Copyright 2019 New York University.
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

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.util.MemUsagePrinter;
import org.urban.data.core.util.StringHelper;
import org.urban.data.db.term.Term;
import org.urban.data.db.term.TermConsumer;

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
public class CompressedTermIndexGenerator implements TermConsumer {

    private final String _domain;
    private HashMap<Integer, HashMap<String, HashIDSet>> _eqIndex = null;
    private final PrintWriter _out;
    private int _termCount = 0;

    public CompressedTermIndexGenerator(PrintWriter out, String domain) {

        _out = out;
        _domain = domain;
        
        _eqIndex = new HashMap<>();
    }

    public CompressedTermIndexGenerator(PrintWriter out) {
        
        this(out, null);
    }
    
    @Override
    public void close() {

        System.out.println(_termCount + " TERMS READ @ " + new java.util.Date());
        
        int counter = 0;
        
        for (HashMap<String, HashIDSet> bucket : _eqIndex.values()) {
            for (String columns : bucket.keySet()) {
                _out.print(counter + "\t");
                boolean isFirst = true;
                for (int termId : bucket.get(columns).toSortedList()) {
                    if (isFirst) {
                        _out.print(termId);
                        isFirst = false;
                    } else {
                        _out.print("," + termId);
                    }
                }
                _out.println("\t" + columns);
                counter++;
            }
        }

        if (_domain != null) {
            System.out.println(_domain + "\t" + _termCount + "\t" + counter);
        } else {
            System.out.println(_termCount + "\t" + counter);
        }
    }

    @Override
    public void consume(Term term) {

        List<Integer> values = term.columns().toSortedList();
        int index = values.get(0);
        String key =  StringHelper.joinIntegers(values);
        
        if (_eqIndex.containsKey(index)) {
            HashMap<String, HashIDSet> bucket = _eqIndex.get(index);
            if (bucket.containsKey(key)) {
                bucket.get(key).add(term.id());
            } else {
                bucket.put(key, new HashIDSet(term.id()));
            }
        } else {
            HashMap<String, HashIDSet> bucket = new HashMap<>();
            bucket.put(key, new HashIDSet(term.id()));
            _eqIndex.put(index, bucket);
        }
        _termCount++;
        if ((_termCount % 100000000) == 0) {
            System.out.println(_termCount + " @ " + new java.util.Date());
            new MemUsagePrinter().print();
        }
    }

    @Override
    public void open() {

        _termCount = 0;
    }
}
