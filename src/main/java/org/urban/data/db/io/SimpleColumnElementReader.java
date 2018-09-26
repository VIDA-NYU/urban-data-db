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
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.column.SimpleColumnElement;

/**
 * Reader for simple column elements.
 * 
 * Reads a default equivalence class file and returns only simple column
 * elements.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleColumnElementReader {

    public IdentifiableObjectSet<SimpleColumnElement> read(File file) throws java.io.IOException {
        
        HashObjectSet<SimpleColumnElement> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                int nodeId = Integer.parseInt(tokens[0]);
                HashIDSet columns = new HashIDSet();
                for (String col : tokens[2].split(",")) {
                    int columnId = Integer.parseInt(
                            col.substring(0, col.indexOf(":"))
                    );
                    columns.add(columnId);
                }
                result.add(new SimpleColumnElement(nodeId, columns));
            }
        }
        
        return result;
    }
}
