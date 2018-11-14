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
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.column.ColumnElement;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.term.ColumnTerm;

/**
 * Equivalence class with mutable list of column statistics.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MutableEquivalenceClass extends IdentifiableObjectImpl implements EquivalenceClass {
    
    private final HashObjectSet<IdentifiableCount> _columns;
    private final HashIDSet _terms;
    
    public MutableEquivalenceClass(
            int id,
            HashIDSet terms,
            IdentifiableObjectSet<IdentifiableCount> columns
    ) {
        
        super(id);
        
        _terms = terms;
        _columns = new HashObjectSet<>(columns);
    }
    
    public MutableEquivalenceClass(int id, HashIDSet terms) {
        
        super(id);
        
        _terms = terms;
        _columns = new HashObjectSet<>();
    }
    
    public void add(ColumnTerm term) {
        
        // If terms a grouped based on similarity of their values prior to
        // grouping them based on the set of columns they occur in a term may
        // be added to an equivalence class that already contains the term. In
        // that case we return immediately.
        if (_terms.contains(term.id())) {
            return;
        }
        
        _terms.add(term.id());
	for (IdentifiableCount col : term.columns()) {
            this.add(col);
	}
    }
    
    public void add(MutableEquivalenceClass eq) {
        
        _terms.add(eq.terms());
	for (IdentifiableCount col : eq.columns()) {
            this.add(col);
	}
    }
    
    public void add(IdentifiableCount col) {
        
        int id = col.id();
        if (_columns.contains(id)) {
            int value = _columns.get(id).count() + col.count();
            _columns.put(new IdentifiableCount(id, value));
        } else {
            _columns.add(col);
        }
    }

    @Override
    public int columnCount() {

        return _columns.length();
    }
    
    @Override
    public IdentifiableObjectSet<IdentifiableCount> columns() {

        return _columns;
    }

    @Override
    public int compareTo(ColumnElement el) {

        return Integer.compare(this.id(), el.id());
    }

    public int remove(int id) {
        
        if (_columns.contains(id)) {
            int count = _columns.get(id).count();
            _columns.remove(id);
            return count;
        } else {
            return 0;
        }
    }
    
    public HashIDSet terms() {
        
        return _terms;
    }

    @Override
    public int termCount() {

        return _terms.length();
    }
    
    /**
     * Print string representation of the equivalence class.
     * 
     * @param out 
     */
    public void write(PrintWriter out) {
	
	out.println(
                this.id() + "\t" +
                _terms.toIntString() + "\t" +
                ColumnElementHelper.toArrayString(_columns.toList())
        );
    }
}
