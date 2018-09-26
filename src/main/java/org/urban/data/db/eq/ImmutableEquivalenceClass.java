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
package org.urban.data.db.eq;

import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.ImmutableObjectSet;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.column.ImmutableColumnElement;

/**
 * Equivalence class with an immutable set of column frequencies.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ImmutableEquivalenceClass extends ImmutableColumnElement implements EquivalenceClass {

    private final int _termCount;
    
    public ImmutableEquivalenceClass(
            int id,
            int termCount,
            ImmutableObjectSet<IdentifiableCount> columns
    ) {

        super(id, columns);

        _termCount = termCount;
    }

    public ImmutableEquivalenceClass(
            int id,
            int termCount,
            String columns
    ) {

        super(id, ColumnElementHelper.fromStringArray(columns));

        _termCount = termCount;
    }

    @Override
    public int termCount() {

        return _termCount;
    }
}
