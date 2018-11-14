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
package org.urban.data.db.io;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.urban.data.core.object.Entity;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 * Get set of entities from term index.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EntitySetReader {
    
    private class EntityConsumer implements TermConsumer {

        private List<Entity> _terms = null;
        
        @Override
        public void close() {

        }

        @Override
        public void consume(ColumnTerm term) {

            _terms.add(new Entity(term.id(), term.value()));
        }

        @Override
        public void open() {

            _terms = new ArrayList<>();
        }
        
        public List<Entity> terms() {
            
            return _terms;
        }
    }
    
    public List<Entity> read(File termFile) throws java.io.IOException {
        
        EntityConsumer consumer = new EntityConsumer();
        new TermIndexReader().read(termFile, consumer);
        return consumer.terms();
    }
}
