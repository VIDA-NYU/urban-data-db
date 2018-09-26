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
package org.urban.data.db.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueCounterImpl;
import org.urban.data.core.io.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleColumnValueReader extends ColumnReader {

    private int _readIndex = 0;
    private final ArrayList<ValueCounter> _values;
    
    public SimpleColumnValueReader(InputStream is, int columnId) throws java.io.IOException {
        
	super(columnId);
	
        _values = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(is))) {
            String line = null;
            while ((line = in.readLine()) != null) {
                _values.add(new ValueCounterImpl(line.toUpperCase(), 1));
            }
        }
    }
    
    public SimpleColumnValueReader(File file, int columnId) throws java.io.IOException {
        
        this(FileSystem.openFile(file), columnId);
    }
    
    public SimpleColumnValueReader(ArrayList<ValueCounter> values, int columnId) {
        
	super(columnId);
	
        _values = values;
    }
    
    @Override
    public ColumnReader cloneReader() {

        return new SimpleColumnValueReader(_values, this.columnId());
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {

        return (_readIndex < _values.size());
    }

    @Override
    public ValueCounter next() {
        
        return _values.get(_readIndex++);
    }

    @Override
    public void reset() {

        _readIndex = 0;
    }
}
