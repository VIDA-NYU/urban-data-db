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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;

/**
 * Set of equivalence class terms.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQTermSet {
    
    private final HashMap<Integer, EQTerm> _elements;
    private final HashIDSet _nodes;
    private final HashIDSet _terms;
    
    public EQTermSet(Iterable<EQTerm> elements) {
        
        _elements = new HashMap<>();
        
        _nodes = new HashIDSet();
        _terms = new HashIDSet();
        
        for (EQTerm term : elements) {
            _elements.put(term.termId(), term);
            _nodes.add(term.nodeId());
            _terms.add(term.termId());
        }
    }
    
    public EQTermSet() {
        
        this(new ArrayList<>());
    }
    
    public IDSet nodes() {
    
        return _nodes;
    }
    
    public EQTermSet sample(int size) {
    
        if (size > _elements.size()) {
            throw new IllegalArgumentException("Cannot sample " + size + " elements from a set of " + _elements.size());
        } else if (size == _elements.size()) {
            return this;
        } else {
            ArrayList<EQTerm> sample = new ArrayList<>();
            for (int termId : _terms.sample(size)) {
                sample.add(_elements.get(termId));
            }
            return new EQTermSet(sample);
        }
    }
    
    public IDSet terms() {
        
        return _terms;
    }
    
    public List<EQTerm> toList() {
        
        return new ArrayList<>(_elements.values());
    }
}
