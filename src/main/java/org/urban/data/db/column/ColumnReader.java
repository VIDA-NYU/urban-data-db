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

import java.util.List;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.eq.EquivalenceClass;
import org.urban.data.db.io.EquivalenceClassReader;

/**
 * Read individual columns from an equivalence class file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ColumnReader<T extends EquivalenceClass> {
    
    private final EquivalenceClassReader<T> _reader;
    
    public ColumnReader(EquivalenceClassReader<T> reader) {
        
        _reader = reader;
    }
    
    public IdentifiableObjectSet<T> get(int columnId) throws java.io.IOException {
        
        ColumnFilter consumer = new ColumnFilter(columnId);
        _reader.read(consumer);
        
        List<T> nodes = consumer.nodes();
        if (!nodes.isEmpty()) {
            return new HashObjectSet<>(nodes);
        } else {
            return null;
        }
    }
}
