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

import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableObjectSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AdvancedEquivalenceClass extends ImmutableEquivalenceClass {
    
    private final IDSet _terms;
    
    public AdvancedEquivalenceClass(
            int id,
            IDSet terms,
            ImmutableObjectSet<IdentifiableCount> columns
    ) {
        
        super(id, terms.length(), columns);
        
        _terms = terms;
    }
    
    public AdvancedEquivalenceClass(
            int id,
            IDSet terms,
            String columns
    ) {
        
        super(id, terms.length(), columns);
        
        _terms = terms;
    }
    
    /**
     * List of term identifier for terms in the equivalence class.
     * 
     * @return 
     */
    public IDSet terms() {
        
        return _terms;
    }
    
    public static IDSet union(Iterable<AdvancedEquivalenceClass> nodes) {
        
        HashIDSet terms = new HashIDSet();
        
        for (AdvancedEquivalenceClass node : nodes) {
            terms.add(node.terms());
        }
        
        return terms;
    }
}
