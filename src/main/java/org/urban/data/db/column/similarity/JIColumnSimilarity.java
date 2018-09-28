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
import org.urban.data.core.similarity.JaccardIndex;
import org.urban.data.db.column.Column;
import org.urban.data.core.set.similarity.SetSimilarityComputer;

/**
 * Computes similarity for a pair of columns as the Jaccard Index of the
 * elements in the column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JIColumnSimilarity implements SetSimilarityComputer<Column> {

    @Override
    public BigDecimal getSimilarity(Column col1, Column col2) {

        int ovp = col1.overlap(col2);
        if (ovp > 0) {
            return JaccardIndex.ji(col1.length(), col2.length(), ovp);
        } else {
            return BigDecimal.ZERO;
        }
    }
    
}
