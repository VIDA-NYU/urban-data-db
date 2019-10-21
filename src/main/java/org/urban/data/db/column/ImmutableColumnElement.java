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
package org.urban.data.db.column;

import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableObjectSet;

/**
 * Immutable column element.
 * 
 * In an immutable column element the set of column frequencies is fixed and
 * cannot be modified.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ImmutableColumnElement extends IdentifiableObjectImpl implements ColumnElement<IdentifiableCount> {

    private final ImmutableObjectSet<IdentifiableCount> _columns;
    
    public ImmutableColumnElement(
            int id,
            ImmutableObjectSet<IdentifiableCount> columns
    ) {
	
	super(id);
        
        _columns = columns;
    }

    @Override
    public int columnCount() {
        
        return _columns.length();
    }
    
    public IdentifiableObjectSet<IdentifiableCount> columnFrequencies() {
        
        return _columns;
    }
    
    @Override
    public IdentifiableObjectSet<IdentifiableCount> columns() {

        return _columns;
    }
    
    @Override
    public int compareTo(ColumnElement el) {
        
        return Integer.compare(this.id(), el.id());
    }
}
