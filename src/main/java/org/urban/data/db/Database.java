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
package org.urban.data.db;

import java.util.Iterator;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.MutableObjectSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.column.Column;
import org.urban.data.db.column.ColumnElement;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class Database<T extends ColumnElement<IdentifiableCount>> implements Iterable<Column> {
    
    private final MutableObjectSet<Column> _columns;
    
    public Database(Iterable<T> nodes) {
        
        _columns = new HashObjectSet<>();
        
        for (T node : nodes) {
            for (IdentifiableCount col : node.columns()) {
                Column column;
                if (!_columns.contains(col.id())) {
                    column = new Column(col.id());
                    _columns.add(column);
                } else {
                    column = _columns.get(col.id());
                }
                column.add(node.id());
            }
        }
    }
    
    public IDSet columnIds() {
    
        HashIDSet columns = new HashIDSet();
        for (Column column : _columns) {
            columns.add(column.id());
        }
        return columns;
    }
    
    public IdentifiableObjectSet<Column> columns() {
        
        return _columns;
    }
    
    public Column get(int columnId) {
        
        return _columns.get(columnId);
    }
    
    @Override
    public Iterator<Column> iterator() {

        return _columns.iterator();
    }
    
    public int size() {
    
        return _columns.length();
    }
}
