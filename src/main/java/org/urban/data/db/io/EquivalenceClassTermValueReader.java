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
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.eq.AdvancedEquivalenceClass;
import org.urban.data.db.eq.EquivalenceClassConsumer;
import org.urban.data.db.eq.EquivalenceClassTerm;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 * Read all term values for a given set of equivalence class nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EquivalenceClassTermValueReader {
    
    private class TermSetCollector implements EquivalenceClassConsumer<AdvancedEquivalenceClass> {

        private final HashObjectSet<EquivalenceClassTerm> _terms;

        private TermSetCollector() {
            
            this._terms = new HashObjectSet<>();
        }
        
        @Override
        public void close() {
        }

        @Override
        public void consume(AdvancedEquivalenceClass eq) {

            for (int termId : eq.terms()) {
                _terms.add(new EquivalenceClassTerm(termId, eq.id()));
            }
        }

        @Override
        public void open() {

        }
        
        public IdentifiableObjectSet<EquivalenceClassTerm> terms() {
            
            return _terms;
        }
    }
    
    private class TermValueCollector implements TermConsumer {

        private final IdentifiableObjectSet<EquivalenceClassTerm> _filter;
        private final HashObjectSet<EquivalenceClassTerm> _terms = new HashObjectSet<>();
        
        public TermValueCollector(IdentifiableObjectSet<EquivalenceClassTerm> filter) {
            
            _filter = filter;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(ColumnTerm term) {

            if (_filter.contains(term.id())) {
                _terms.add(
                        new EquivalenceClassTerm(
                                term.id(),
                                term.value(),
                                _filter.get(term.id()).eqId()
                        )
                );
            }
        }

        @Override
        public void open() {

        }
        
        public IdentifiableObjectSet<EquivalenceClassTerm> terms() {
            
            return _terms;
        }
    }
    
    private final File _eqFile;
    private final File _termIndexFile;
    
    public EquivalenceClassTermValueReader(File eqFile, File termIndexFile) {
        
        _eqFile = eqFile;
        _termIndexFile = termIndexFile;
    }
    
    public IdentifiableObjectSet<EquivalenceClassTerm> read(IDSet nodes) throws java.io.IOException {
        
        TermSetCollector termCollector = new TermSetCollector();
        new AdvancedEquivalenceClassReader().read(_eqFile, termCollector, nodes);
        
        TermValueCollector valueCollector;
        valueCollector = new TermValueCollector(termCollector.terms());
        
        new TermIndexReader().read(_termIndexFile, valueCollector);
        
        return valueCollector.terms();
    }
}
