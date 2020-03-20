/*
 * Copyright 2020 New York University.
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

import org.urban.data.core.set.IDSet;

/**
 * Reduce the set of columns each term occurs in to a given filter set of columns.
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermColumnProjection implements TermConsumer {

    private final TermConsumer _consumer;
    private final IDSet _filter;
    
    public TermColumnProjection(IDSet filter, TermConsumer consumer) {
        
        _filter = filter;
        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }

    @Override
    public void consume(Term term) {

        IDSet columns = term.columns().intersect(_filter);
        if (!columns.isEmpty()) {
            _consumer.consume(new Term(term.id(), term.name(), columns));
        }
    }

    @Override
    public void open() {

        _consumer.open();
    }
}
