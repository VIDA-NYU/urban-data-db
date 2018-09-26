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
package org.urban.data.db.column;

import java.util.HashMap;
import java.util.Set;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.util.count.Counter;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.eq.EquivalenceClass;

/**
 * Maintain a pair of counter for each column. Counts the number of distinct and
 * total terms / equivalence classes in the column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ColumnStats<T extends EquivalenceClass> {
 
    private final static int DISTINCT_INDEX = 0;
    private final static int TOTAL_INDEX = 1;
    private final static int MAX_INDEX = 2;
    
    private final HashMap<Integer, Counter[]> _elements = new HashMap<>();

    public ColumnStats(IdentifiableObjectSet<T> nodes) {
        
        for (T el : nodes) {
            for (IdentifiableCount col : el.columns()) {
                this.inc(col.id(), col.count());
            }
        }
    }
    
    public int columnCount() {
        
        return _elements.size();
    }
    
    private int get(int columnId, int index) {
        
        if (_elements.containsKey(columnId)) {
            return _elements.get(columnId)[index].value();
        } else {
            return 0;
        }
    }
    
    public int getDistinctCount(int columnId) {
        
        return this.get(columnId, DISTINCT_INDEX);
    }
    
    public int getMaxCount(int columnId) {
        
        return this.get(columnId, MAX_INDEX);
    }
    
    public int getTotalCount(int columnId) {
        
        return this.get(columnId, TOTAL_INDEX);
    }

    public final void inc(int id, int value) {
        
        if (_elements.containsKey(id)) {
            Counter[] counters = _elements.get(id);
            counters[DISTINCT_INDEX].inc();
            counters[TOTAL_INDEX].inc(value);
            if (value > counters[MAX_INDEX].value()) {
                counters[MAX_INDEX].setValue(value);
            }
        } else {
            Counter[] counters = new Counter[3];
            counters[DISTINCT_INDEX] = new Counter(1);
            counters[TOTAL_INDEX] = new Counter(value);
            counters[MAX_INDEX] = new Counter(value);
            _elements.put(id, counters);
        }
    }
    
    public final Set<Integer> keys() {
        
        return _elements.keySet();
    }
}
