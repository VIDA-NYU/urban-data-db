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
package org.urban.data.db.term;

import java.io.File;
import java.util.HashMap;
import org.urban.data.core.object.Entity;
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.db.io.TermIndexReader;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndex implements TermConsumer {

    private final ObjectFilter<Integer> _filter;
    private final HashMap<Integer, String> _terms = new HashMap<>();
    
    public TermIndex(ObjectFilter<Integer> filter) {
        
        _filter = filter;
    }
    
    public TermIndex() {
        
        this(new AnyObjectFilter<Integer>());
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ColumnTerm term) {

        if (_filter.contains(term.id())) {
            _terms.put(term.id(), term.value());
        }
    }
    
    public Entity get(int id) {
        
        if (_terms.containsKey(id)) {
            return new Entity(id, _terms.get(id));
        } else {
            return null;
        }
    }

    public Entity get(String term) {
    
        for (int id : _terms.keySet()) {
            if (_terms.get(id).equals(term)) {
                return new Entity(id, term);
            }
        }
        return null;
    }
    
    @Override
    public void open() {

        _terms.clear();
    }
    
    public TermIndex read(File file) throws java.io.IOException {
    
        new TermIndexReader(file).read(this);
        
        return this;
    }
}
