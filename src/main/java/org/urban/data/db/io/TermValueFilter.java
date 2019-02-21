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
package org.urban.data.db.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.urban.data.core.set.StringSet;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 * Filter terms that match those in a given string set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermValueFilter implements TermConsumer {

    private final StringSet _filter;
    private final List<ColumnTerm> _terms;
    
    public TermValueFilter(StringSet filter) {
        _filter = filter;
        _terms =  new ArrayList<>();
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ColumnTerm term) {

        if (_filter.contains(term.value())) {
            _terms.add(term);
        }
    }

    @Override
    public void open() {

        _terms.clear();
    }
    
    public List<ColumnTerm> terms() {
        
        return _terms;
    }
    
    public HashMap<String, ColumnTerm> termIndex() {
        
        HashMap<String, ColumnTerm> result = new HashMap<>();
        
        for (ColumnTerm term : _terms) {
            result.put(term.value(), term);
        }
        
        return result;
    }
}
