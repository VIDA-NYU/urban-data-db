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

import java.io.PrintWriter;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableIDSet;

/**
 * Expanded column has an additional set of nodes that were added to it.
 * 
 * The expansion set can be empty.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumn extends Column {
    
    private IDSet _expansion;
    private IDSet _fullNodeSet = null;
    
    public ExpandedColumn(Column column, IDSet expansion) {
        
        super(column.id(), column);
        
        _expansion = expansion;
    }
    
    public ExpandedColumn(Column column) {
        
        this(column, new HashIDSet());
    }
    
    public ExpandedColumn(String line) {
        
        this(parse(line));
        
        String[] tokens = line.split("\t");
        
        if (tokens.length == 3) {
            _expansion = new ImmutableIDSet(tokens[2]);
        }
    }
    
    public IDSet expansion() {
        
        return _expansion;
    }
    
    public IDSet expandedNodeSet() {
        
        if (_fullNodeSet == null) {
            _fullNodeSet = this.union(_expansion);
        }
        return _fullNodeSet;
    }

    public IDSet originalColumn() {
        
        return this;
    }
    
    private static Column parse(String line) {
        
        String[] tokens = line.split("\t");
        
        if ((tokens.length >= 2) && (tokens.length <= 3)) {
            return new Column(
                    Integer.parseInt(tokens[0]),
                    new ImmutableIDSet(tokens[1])
            );
        } else {
            throw new java.lang.IllegalArgumentException("Invalid file format: " + line);
        }
    }
    
    public void write(PrintWriter out) {
        
        String line = this.id() + "\t" + this.toIntString();
        if (!_expansion.isEmpty()) {
            line += "\t" + _expansion.toIntString();
        }
        out.println(line);
    }
}
