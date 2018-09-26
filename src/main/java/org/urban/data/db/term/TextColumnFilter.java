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

import org.urban.data.core.constraint.ThresholdConstraint;
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.util.count.IdentifiableCount;

/**
 * Term consumer that collects the unique identifier of all columns that
 * contain at least one text term.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextColumnFilter implements TermConsumer {

    private class ColumnValueCounter extends IdentifiableObjectImpl {
        
        private int _textValues;
        private int _totalValues;
        
        public ColumnValueCounter(IdentifiableCount column, boolean isText) {
        
            super(column.id());
            
            _totalValues = 1;
            if (isText) {
                _textValues = 1;
            } else {
                _textValues = 0;
            }
        }
        
        public void addText() {
            
            _textValues++;
            _totalValues++;
        }
        
        public void addValue() {
            
            _totalValues++;
        }
        
        public double textValueFraction() {
            
            return (double)_textValues/(double)_totalValues;
        }
    }
    
    private HashObjectSet<ColumnValueCounter> _columns = null;
    
    @Override
    public void close() {

    }

    /**
     * Get set of identifier for text columns that satisfy the given text
     * fraction constraint.
     * 
     * @param threshold
     * @return 
     */
    public IDSet columns(ThresholdConstraint threshold) {
        
        HashIDSet result = new HashIDSet();
        
        for (ColumnValueCounter column : _columns) {
            if (threshold.isSatisfied(column.textValueFraction())) {
                result.add(column.id());
            }
        }
        
        return result;
    }
    
    @Override
    public void consume(ColumnTerm term) {

        boolean isText = term.type().isText();
        for (IdentifiableCount col : term.columns()) {
            if (!_columns.contains(col.id())) {
                _columns.add(new ColumnValueCounter(col, isText));
            } else if (isText) {
                _columns.get(col.id()).addText();
            } else {
                _columns.get(col.id()).addValue();
            }
        }
    }

    @Override
    public void open() {

        _columns = new HashObjectSet<>();
    }
}
