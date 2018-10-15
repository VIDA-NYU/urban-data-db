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

import java.io.BufferedReader;
import java.io.File;
import org.urban.data.core.graph.components.UndirectedConnectedComponents;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableIDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ObjectCollection;

/**
 * Methods for grouping and reading sets of expanded columns from file.
 * 
 * The expansion set can be empty.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnSet {
    
    public IdentifiableObjectSet<ObjectCollection<ExpandedColumn>> group(
            IdentifiableObjectSet<ExpandedColumn> columns
    ) {
        HashObjectSet<ObjectCollection<ExpandedColumn>> result;
        result = new HashObjectSet<>();
        
        UndirectedConnectedComponents compGen;
        compGen = new UndirectedConnectedComponents(columns.keys());
        for (ExpandedColumn colI : columns) {
            for (ExpandedColumn colJ : columns) {
                if (colI.id() < colJ.id()) {
                    if ((colI.sameSetAs(colJ)) && (colI.expansion().sameSetAs(colJ.expansion()))) {
                        compGen.edge(colI.id(), colJ.id());
                    }
                }
            }
        }
        
        for (IdentifiableIDSet comp : compGen.getComponents()) {
            ObjectCollection<ExpandedColumn> group;
            group = new ObjectCollection<>(comp.first());
            for (int columnId : comp) {
                group.add(columns.get(columnId));
            }
            result.add(group);
        }
        
        return result;
    }

    public IdentifiableObjectSet<ObjectCollection<ExpandedColumn>> group(
            File file
    ) throws java.io.IOException {
        
        return group(read(file));
    }

    public IdentifiableObjectSet<ExpandedColumn> read(
            File file
    ) throws java.io.IOException {

        HashObjectSet<ExpandedColumn> columns;
	columns = new HashObjectSet<>();
        try (BufferedReader in = FileSystem.openReader(file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		columns.add(new ExpandedColumn(line));
	    }
	}
        return columns;
    }
}
