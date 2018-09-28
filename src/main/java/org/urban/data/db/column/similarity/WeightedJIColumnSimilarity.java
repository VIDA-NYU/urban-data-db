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

import java.math.BigDecimal;
import java.math.MathContext;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.column.Column;
import org.urban.data.db.column.WeightedColumnElement;
import org.urban.data.core.set.similarity.SetSimilarityComputer;

/**
 * Computes similarity for a pair of columns as the weighted Jaccard Index
 * of the elements in the column.
 * 
 * Element weights for each column are provided by the given weight function.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class WeightedJIColumnSimilarity implements SetSimilarityComputer<Column> {

    private final IdentifiableObjectSet<WeightedColumnElement> _weights;
    
    public WeightedJIColumnSimilarity(
            IdentifiableObjectSet<WeightedColumnElement> weights
    ) {
        _weights = weights;
    }
    
    @Override
    public BigDecimal getSimilarity(Column col1, Column col2) {

	int overlap = 0;
        
        double val1 = 0;
        double val2 = 0;
        double ovpVal = 0;

        for (int el1 : col1) {
            if (col2.contains(el1)) {
                ovpVal += _weights.get(el1).columns().get(col1.id()).value() + _weights.get(el1).columns().get(col2.id()).value();
                overlap++;
            } else {
                 val1 += _weights.get(el1).columns().get(col1.id()).value();
            }
        }
        for (int el2 : col2) {
            if (!col1.contains(el2)) {
                val2 += _weights.get(el2).columns().get(col2.id()).value();
            }
        }
        
        if (overlap > 0) {
            return new BigDecimal(ovpVal)
                    .divide(
                            new BigDecimal(val1 + val2 + ovpVal),
                            MathContext.DECIMAL64
                    );
        } else {
            return BigDecimal.ZERO;
        }
    }
    
}
