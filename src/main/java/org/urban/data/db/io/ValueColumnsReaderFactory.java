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

import java.io.File;
import java.util.LinkedList;
import org.urban.data.core.set.IDSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueColumnsReaderFactory extends ColumnReaderFactory {

    private final LinkedList<File> _files;
    
    public ValueColumnsReaderFactory(File directory) {
	
	_files = new LinkedList<>();
	
	for (File file : directory.listFiles()) {
	    if ((file.getName().endsWith(".txt")) || (file.getName().endsWith(".txt.gz"))) {
		_files.add(file);
	    }
	}
    }
    
    public ValueColumnsReaderFactory(File directory, IDSet filter) {
	
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
    
    @Override
    public boolean hasNext() {

	return (!_files.isEmpty());
    }

    @Override
    public ColumnReader next() {

	File file = _files.pop();
	int columnId = this.getColumnId(file);
	return new FlexibleColumnReader(file, columnId);
    }
}
