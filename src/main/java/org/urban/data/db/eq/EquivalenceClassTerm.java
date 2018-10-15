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

import org.urban.data.core.object.IdentifiableObjectImpl;

/**
 * Term in an equivalence class.
 * 
 * Contains the term value as well as the identifier of the equivalence class
 * and the term.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EquivalenceClassTerm extends IdentifiableObjectImpl implements Comparable<EquivalenceClassTerm> {

    private final int _eqId;
    private final String _value;
    
    public EquivalenceClassTerm(int termId, String value, int eqId) {
    
        super(termId);
        
        _eqId = eqId;
        _value = value;
    }
    
    public EquivalenceClassTerm(int termId, int eqId) {
        
        this(termId, null, eqId);
    }

    @Override
    public int compareTo(EquivalenceClassTerm term) {

        return this.value().compareTo(term.value());
    }
    
    public int eqId() {
        
        return _eqId;
    }
    
    public int termId() {
        
        return this.id();
    }
    
    public String value() {

        return _value;
    }
}
