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

import org.urban.data.core.object.NamedObject;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.set.ImmutableObjectSet;
import org.urban.data.core.value.profiling.types.DataTypeLabel;
import org.urban.data.db.column.ColumnElementHelper;
import org.urban.data.db.column.ImmutableColumnElement;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnTerm extends ImmutableColumnElement implements NamedObject {
    
    private final String _term;
    private final DataTypeLabel _type;
    
    public ColumnTerm(
            int id,
            String term,
            DataTypeLabel type,
            ImmutableObjectSet<IdentifiableCount> columns
    ) {
        
        super(id, columns);
        
        _term = term;
        _type = type;
    }
    
    /**
     * Initialize column term when reading from term index file.
     * 
     * The list of column counts is expected to be of the form
     * columnId:frequency.
     * 
     * @param id
     * @param term
     * @param type
     * @param columnCounts 
     */
    public ColumnTerm(
            int id,
            String term,
            DataTypeLabel type,
            String columnCounts
    ) {
        this(id, term, type, ColumnElementHelper.fromStringArray(columnCounts));
    }
    
    @Override
    public String name() {
        
        return _term;
    }
    
    /**
     * The actual term value.
     * 
     * @return 
     */
    public String value() {
        
        return _term;
    }
    
    /**
     * Assigned data type for the term.
     * 
     * @return 
     */
    public DataTypeLabel type() {
        
        return _type;
    }
}
