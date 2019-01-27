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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.urban.data.core.object.Entity;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.sort.EntityNameSort;
import org.urban.data.db.io.TermIndexReader;

/**
 * Group terms that are identical with respect to the sequence of letters and
 * digits in their value.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermGroupGenerator {
    
    private class TermCollector implements TermConsumer {

        private List<Entity> _terms = null;
        
        @Override
        public void close() {

        }

        @Override
        public void consume(ColumnTerm term) {

            StringBuilder buf = new StringBuilder();
            boolean hasAlpha = false;
            for (char c : term.value().toCharArray()) {
                if (Character.isAlphabetic(c)) {
                    buf.append(c);
                    hasAlpha = true;
                } else if (Character.isDigit(c)) {
                    buf.append(c);                    
                }
            }
            if (hasAlpha) {
                _terms.add(new Entity(term.id(), buf.toString()));
            } else {
                _terms.add(new Entity(term.id(), term.value()));
            }
        }

        @Override
        public void open() {

            _terms = new ArrayList<>();
        }
        
        public List<Entity> terms() {
            
            return _terms;
        }
    }
    
    public List<IDSet> run(File termFile) throws java.io.IOException {
        
        TermCollector consumer = new TermCollector();
        new TermIndexReader(termFile).read(consumer);
        List<Entity> terms = consumer.terms();
        
        List<IDSet> result = new ArrayList<>();
        if (!terms.isEmpty()) {
            Collections.sort(terms, new EntityNameSort());
            Entity prev = terms.get(0);
            HashIDSet bucket = null;
            for (int iTerm = 1; iTerm < terms.size(); iTerm++) {
                Entity term = terms.get(iTerm);
                if (term.name().equals(prev.name())) {
                    if (bucket == null) {
                        bucket = new HashIDSet(prev.id());
                        result.add(bucket);
                    }
                    bucket.add(term.id());
                } else {
                    if (bucket != null) {
                        bucket = null;
                    }
                }
                prev = term;
            }
        }
        return result;
    }
}
