/*
 * Copyright 2018 Heiko Mueller <heiko.mueller@nyu.edu>.
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
package org.urban.data.db.eq;

import org.urban.data.core.util.count.IdentifiableCount;

/**
 * Equivalence class consumer that collects statistics about database.
 * 
 * Records the number of equivalence classes, terms, and non-empty cells in the
 * database.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EquivalenceClassStatsConsumer implements EquivalenceClassConsumer {

    private long _cellCount = 0;
    private int _equivalenceClassCount = 0;
    private int _termCount = 0;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(EquivalenceClass eq) {

	_equivalenceClassCount++;
	_termCount += eq.termCount();
	for (IdentifiableCount col : eq.columns()) {
	    _cellCount += col.count();
	}
    }

    public long getCellCount() {
	
	return _cellCount;
    }
    
    public int getEquivalenceClassCount() {
	
	return _equivalenceClassCount;
    }
    
    public int getTermCount() {
	
	return _termCount;
    }
    
    @Override
    public void open() {

	_cellCount = 0;
	_equivalenceClassCount = 0;
	_termCount = 0;
    }
}
