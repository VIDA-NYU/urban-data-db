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
package org.urban.data.db.term;

import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;

/**
 * Get term identifier for all terms that are classified as TEXT.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextTermIDQuery implements TermConsumer {

    private final HashIDSet _terms;
    
    public TextTermIDQuery() {
        
        _terms = new HashIDSet();
    }
    @Override
    public void close() {

    }

    @Override
    public void consume(ColumnTerm term) {

        if (term.type().isText()) {
            _terms.add(term.id());
        }
    }

    @Override
    public void open() {

        _terms.clear();
    }
    
    public IDSet terms() {
        
        return _terms;
    }
}
