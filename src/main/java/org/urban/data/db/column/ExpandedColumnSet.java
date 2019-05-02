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

import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableIDSet;

/**
 * Group of identical expanded columns.
 * 
 * The expansion set can be empty.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnSet extends IdentifiableObjectImpl {
    
    private final IDSet _columns;
    private final ImmutableIDSet _expansion;
    private final ImmutableIDSet _fullNodeSet;
    private final ImmutableIDSet _nodes;
    
    public ExpandedColumnSet(IDSet columns, ImmutableIDSet nodes, ImmutableIDSet expansion) {
        
        super(columns.first());
        
        _columns = columns;
        _nodes = nodes;
        _expansion = expansion;
        _fullNodeSet = _nodes.union(_expansion);
    }
    
    public IDSet columns() {
        
        return _columns;
    }
    
    public ImmutableIDSet expansion() {
        
        return _expansion;
    }
    
    public ImmutableIDSet expandedNodeSet() {
        
        return _fullNodeSet;
    }
    
    public ImmutableIDSet nodes() {
        
        return _nodes;
    }
}
