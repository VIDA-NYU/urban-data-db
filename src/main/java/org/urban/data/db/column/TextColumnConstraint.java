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
package org.urban.data.db.column;

import java.math.BigDecimal;
import org.urban.data.core.constraint.ThresholdConstraint;
import org.urban.data.core.value.profiling.types.DataTypeLabel;

/**
 * Column type constraint for text columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextColumnConstraint implements ColumnTypeConstraint {

    private int _matchCount = 0;
    private final ThresholdConstraint _threshold;
    private int _totalCount = 0;

    public TextColumnConstraint(ThresholdConstraint threshold) {

        _threshold = threshold;
    }

    @Override
    public void consume(DataTypeLabel type) {
        _totalCount++;
        if (type.isText()) {
            _matchCount++;
        }
    }

    @Override
    public boolean isSatisfied() {

        if (_totalCount > 0) {
            return _threshold.isSatisfied((double)_matchCount / (double)_totalCount);
        } else {
            return _threshold.isSatisfied(BigDecimal.ZERO);
        }
    }

    @Override
    public void reset() {

        _matchCount = 0;
        _totalCount = 0;
    }
}
