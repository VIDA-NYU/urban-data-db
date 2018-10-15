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
import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableIDSet;

/**
 * An expanded column column equivalence class contains two sets of nodes.
 * 
 * Contains the original set of nodes in the column and the full set of nodes in
 * the expanded column (including the original nodes).
 * 
 * Also contains the list of identifier for columns in the equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnEquivalenceClass extends IdentifiableObjectImpl {
   
    private final ColumnEquivalenceClass _column;
    private final IDSet _expansion;
    private IDSet _expandedNodeSet = null;
    
    public ExpandedColumnEquivalenceClass(ColumnEquivalenceClass column, IDSet expansion) {
	
	super(column.id());
	
	_column = column;
	_expansion = expansion;
    }
    
    public ExpandedColumnEquivalenceClass(ColumnEquivalenceClass column) {

	this(column, column);
    }
    
    public ExpandedColumnEquivalenceClass(String[] tokens) {
	
	super(Integer.parseInt(tokens[0]));
	
	_column = new ColumnEquivalenceClass(
		Integer.parseInt(tokens[0]),
		new ImmutableIDSet(tokens[2]),
		new ImmutableIDSet(tokens[1])
	);
	if (tokens.length == 4) {
	    _expansion = new ImmutableIDSet(tokens[3]);
	} else {
	    _expansion = new ImmutableIDSet();
	}
    }
    
    public ExpandedColumnEquivalenceClass(String value) {
	
	this(value.split("\t"));
    }
    
    public IDSet columns() {
	
	return _column.columns();
    }
    
    public IDSet expandedNodeSet() {
	
	if (_expandedNodeSet == null) {
	    _expandedNodeSet = _column.union(_expansion);
	}
	return _expandedNodeSet;
    }
    
    public IDSet expansion() {
	
	return _expansion;
    }

    public int length() {
	
	return _column.length() + _expansion.length();
    }
    
    public IDSet originalColumn() {
	
	return _column;
    }

    public void write(PrintWriter out) {
	
	out.println(
		this.id() + "\t" +
		_column.columns().toIntString() + "\t" +
		_column.toIntString() + "\t" +
		_expansion.toIntString()
	);
    }
}
