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

import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableIdentifiableIDSet;

/**
 * Group of columns that contain the same set of nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnEquivalenceClass extends ImmutableIdentifiableIDSet {
    
    private final IDSet _columns;
    
    public ColumnEquivalenceClass(int id, IDSet nodes, IDSet columns) {
        
        super(id, nodes);
        
        _columns = columns;
    }
    
    public IDSet columns() {
        
        return _columns;
    }
}
