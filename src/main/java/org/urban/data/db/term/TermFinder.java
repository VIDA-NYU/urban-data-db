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
import org.urban.data.core.object.Entity;
import org.urban.data.db.io.TermIndexReader;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermFinder implements TermConsumer {

    private Entity _term = null;
    private final String _value;
    
    public TermFinder(String value) {
        
        _value = value;
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ColumnTerm term) {

        if (_term == null) {
            if (term.value().equals(_value)) {
                _term = new Entity(term.id(), term.value());
            }
        }
    }

    public Entity find(File file) throws java.io.IOException {
    
        new TermIndexReader().read(file, this);
        
        return _term;
    }
    
    @Override
    public void open() {

        _term = null;
    }
}
