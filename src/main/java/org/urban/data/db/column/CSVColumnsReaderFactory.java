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

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.urban.data.core.value.ValueCounter;

/**
 * Reader factory for column files. Uses the flexible column reader.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CSVColumnsReaderFactory implements ColumnReaderFactory {

    private final LinkedList<File> _files;
    
    public CSVColumnsReaderFactory(List<File> files) {
        
        _files = new LinkedList<>(files);
    }
    
    @Override
    public boolean hasNext() {

        return (!_files.isEmpty());
    }

    @Override
    public ColumnReader<ValueCounter> next() {

        File file = _files.pop();
        return new CSVColumnReader(file, ColumnHelper.getColumnId(file));
    }
}