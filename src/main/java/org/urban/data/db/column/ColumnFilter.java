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
package org.urban.data.db.column;

import java.util.ArrayList;
import java.util.List;
import org.urban.data.db.eq.EquivalenceClass;
import org.urban.data.db.eq.EquivalenceClassConsumer;

/**
 * Consumer that filters the set of equivalence classes belonging to a given
 * column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ColumnFilter<T extends EquivalenceClass> implements EquivalenceClassConsumer<T> {

    private final int _columnId;
    private final List<T> _nodes;
    
    public ColumnFilter(int columnId) {
        
        _columnId = columnId;
        
        _nodes = new ArrayList<>();
    }
    @Override
    public void close() {
    }

    @Override
    public void consume(T node) {

        if (node.columns().contains(_columnId)) {
            _nodes.add(node);
        }
    }
    
    public List<T> nodes() {
        
        return _nodes;
    }

    @Override
    public void open() {
        
        if (!_nodes.isEmpty()) {
            _nodes.clear();
        }
    }
}
