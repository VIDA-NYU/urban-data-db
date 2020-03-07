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
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.util.count.Counter;
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

    private final Counter _counter;
    private final String _domain;
    private HashMap<String, MutableEQ> _eqIndex = null;
    private final PrintWriter _out;
    private int _termCount = 0;

    public CompressedTermIndexGenerator(PrintWriter out, String domain) {

        _out = out;
        _domain = domain;
        
        _eqIndex = new HashMap<>();
        _counter = new Counter(0);
    }

    public CompressedTermIndexGenerator(PrintWriter out) {
        
        this(out, null);
    }
    
    @Override
    public void close() {

        for (MutableEQ eq : _eqIndex.values()) {
            eq.write(_out);
        }

        if (_domain != null) {
            System.out.println(_domain + "\t" + _termCount + "\t" + _eqIndex.size());
        } else {
            System.out.println(_termCount + "\t" + _eqIndex.size());
        }
    }

    @Override
    public void consume(Term term) {

        String key = term.columns().toIntString();
        if (_eqIndex.containsKey(key)) {
            _eqIndex.get(key).add(term);
        } else {
            HashIDSet terms = new HashIDSet();
            terms.add(term.id());
            _eqIndex.put(
                    key,
                    new MutableEQ(_counter.inc(), term)
            );
        }
        _termCount++;
    }

    @Override
    public void open() {

        _termCount = 0;
    }
}
