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
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.value.ValueCounter;

/**
 * Reader factory for column files. Uses the flexible column reader.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueColumnsReaderFactory extends ColumnReaderFactory {

    private final LinkedList<File> _files;
    
    public ValueColumnsReaderFactory(File directory, ObjectFilter<Integer> filter) {
	
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory " + directory.getAbsolutePath() + " does not exist");
        } else if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directory.getAbsolutePath() + " not a directory");
        }
        
        _files = new LinkedList<>();

        for (File file : directory.listFiles()) {
            if ((file.getName().endsWith(".txt")) || (file.getName().endsWith(".txt.gz"))) {
                    int columnId = this.getColumnId(file);
                    if (filter.contains(columnId)) {
                        _files.add(file);
                    }
            }
        }
    }
    
    public ValueColumnsReaderFactory(File directory) {
	
        this(directory, new AnyObjectFilter<Integer>());
    }
    
    public ValueColumnsReaderFactory(List<File> files) {
        
        _files = new LinkedList<>(files);
    }
    
    @Override
    public boolean hasNext() {

        return (!_files.isEmpty());
    }

    @Override
    public ColumnReader<ValueCounter> next() {

        File file = _files.pop();
        int columnId = this.getColumnId(file);
        return new SimpleColumnReader(file, columnId);
    }
}
