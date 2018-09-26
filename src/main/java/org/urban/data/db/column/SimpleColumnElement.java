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

import org.urban.data.core.object.IdentifiableObject;
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableObjectSet;

/**
 * Basic column element that contains the object identifier and a set of 
 * column identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleColumnElement extends IdentifiableObjectImpl implements ColumnElement<IdentifiableObject> {

    private final IdentifiableObjectSet<IdentifiableObject> _columns;
    
    public SimpleColumnElement(int id, IDSet columns) {
        
        super(id);
        
        IdentifiableObject[] elements = new IdentifiableObject[columns.length()];
        int index = 0;
        for (int columnId : columns) {
            elements[index++] = new IdentifiableObjectImpl(columnId);
        }
        _columns = new ImmutableObjectSet(elements);
    }
    
    @Override
    public int columnCount() {

        return _columns.length();
    }

    @Override
    public IdentifiableObjectSet<IdentifiableObject> columns() {

        return _columns;
    }
    
    @Override
    public int compareTo(ColumnElement el) {
        
        return Integer.compare(this.id(), el.id());
    }
}
