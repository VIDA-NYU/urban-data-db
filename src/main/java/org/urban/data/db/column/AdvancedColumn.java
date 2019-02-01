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

import java.io.File;
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.eq.AdvancedEquivalenceClass;

/**
 * An advanced column contains the column nodes and column terms. The column
 * nodes are maintained as advanced equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AdvancedColumn extends IdentifiableObjectImpl {
   
    private final IdentifiableObjectSet<AdvancedEquivalenceClass> _nodes;
    private final IDSet _terms;
    
    public AdvancedColumn(int id, File file) throws java.io.IOException {
        
        super(id);
        
        _nodes = new AdvancedColumnReader(file).get(id);
        _terms = AdvancedEquivalenceClass.union(_nodes);
    }
    
    public IdentifiableObjectSet<AdvancedEquivalenceClass> nodes() {
        
        return _nodes;
    }
    
    public IDSet terms() {
        
        return _terms;
    }
}
