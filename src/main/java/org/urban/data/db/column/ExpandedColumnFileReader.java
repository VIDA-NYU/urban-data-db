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

import java.io.BufferedReader;
import java.io.File;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.column.ExpandedColumn;

/**
 * Read a set of expanded column from file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnFileReader {
    
    private final File _file;
    
    public ExpandedColumnFileReader(File file) {
        
        _file = file;
    }
    
    public IdentifiableObjectSet<ExpandedColumn> read() throws java.io.IOException {

        HashObjectSet<ExpandedColumn> columns;
	columns = new HashObjectSet<>();
        try (BufferedReader in = FileSystem.openReader(_file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		columns.add(ExpandedColumn.parse(line));
	    }
	}
        
        return columns;
    }

}
