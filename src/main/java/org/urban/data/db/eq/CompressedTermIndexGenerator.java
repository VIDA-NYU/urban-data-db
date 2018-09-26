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

import java.io.PrintWriter;
import java.util.HashMap;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.term.TermConsumer;

/**
 * Compress a term index into a set of equivalence classes.
 * 
 * The observeFrequencies flag determines if equivalence classes will contain
 * terms that always occur together in the same columns without considering 
 * their frequency of occurrence or whether frequencies are taken into account,
 * i.e., the terms occur in the same columns always with the same frequency.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CompressedTermIndexGenerator implements TermConsumer {
    
    private final int _columnThreshold;
    private HashMap<String, MutableEquivalenceClass> _eqIndex = null;
    private final boolean _observeFrequencies;
    private final PrintWriter _out;

    public CompressedTermIndexGenerator(
            int columnThreshold,
            boolean observeFrequencies,
            PrintWriter out
    ) {
        _columnThreshold = columnThreshold;
        _observeFrequencies = observeFrequencies;
	_out = out;
    }
    public CompressedTermIndexGenerator(
            boolean observeFrequencies,
            PrintWriter out
    ) {
        this(0, observeFrequencies, out);
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
				_eqIndex.size(),
				terms,
				term.columnFrequencies()
			)
		);
            }
        }
    }

    @Override
    public void open() {

        _eqIndex = new HashMap<>();
    }
}
