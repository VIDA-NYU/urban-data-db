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
package org.urban.data.db.column.similarity;

import java.io.File;
import org.urban.data.core.object.IdentifiableDouble;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableObjectSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.util.count.IdentifiableCounterSet;
import org.urban.data.db.column.WeightedColumnElement;
import org.urban.data.db.io.TermIndexReader;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 * Generate set of normalized column weights for all elements in a database.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class ColumnWeightFactory {

    private class TermWeightGenerator implements TermConsumer {

        private final IdentifiableCounterSet _columnScales;
        private HashObjectSet<WeightedColumnElement> _weightedTerms;
        
        public TermWeightGenerator(IdentifiableCounterSet columnScales) {
            
            _columnScales = columnScales;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(ColumnTerm term) {

            IdentifiableDouble[] weights;
            weights = new IdentifiableDouble[term.columnCount()];
            int index = 0;
            for (IdentifiableCount col : term.columns()) {
                int scale = _columnScales.get(col.id()).value();
                weights[index++] = new IdentifiableDouble(
                        col.id(),
                        (double)col.count() / (double)scale
                );
            }
	    _weightedTerms.add(
                    new WeightedColumnElement(
                            term.id(),
                            new ImmutableObjectSet<>(weights)
                    )
            );
        }
        
        public IdentifiableObjectSet<WeightedColumnElement> getWeights() {
            
            return _weightedTerms;
        }

        @Override
        public void open() {

            _weightedTerms = new HashObjectSet<>();
        }
    }
    
    public abstract IdentifiableCounterSet getScales(File file) throws java.io.IOException;
    
    public IdentifiableObjectSet<WeightedColumnElement> getWeights(File file) throws java.io.IOException {
        
        IdentifiableCounterSet columnScales = this.getScales(file);
        TermWeightGenerator consumer = new TermWeightGenerator(columnScales);
        new TermIndexReader().read(file, consumer);
        return consumer.getWeights();
    }
}
