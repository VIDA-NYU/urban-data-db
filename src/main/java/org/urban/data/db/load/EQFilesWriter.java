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
package org.urban.data.db.load;

import java.io.PrintWriter;
import java.util.List;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.MutableIDSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.eq.AdvancedEquivalenceClass;
import org.urban.data.db.eq.EquivalenceClassConsumer;

/**
 * Write equivalence classes to file.
 * 
 * Reduces the number of terms per equivalence class if a term threshold is
 * given (i.e., the value is not negative).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQFilesWriter implements EquivalenceClassConsumer<AdvancedEquivalenceClass> {

    private final PrintWriter _outColumnNodeMap;
    private final PrintWriter _outTermNodeMap;
    private final MutableIDSet _terms;
    private final int _termThreshold;
    
    public EQFilesWriter(
	    PrintWriter outColumnNodeMap,
	    PrintWriter outTermNodeMap,
	    int termThreshold
    ) {
        
        _outColumnNodeMap = outColumnNodeMap;
        _outTermNodeMap = outTermNodeMap;
	_termThreshold = termThreshold;
	
	_terms = new HashIDSet();
    }
    
    @Override
    public void close() {
        
    }

    @Override
    public void consume(AdvancedEquivalenceClass eq) {

        for (IdentifiableCount col : eq.columns()) {
            _outColumnNodeMap.println(
                    col.id() + "\t" + eq.id() + "\t" + col.count()
            );
        }
	
	// The set of terms that are written to output depends on the number of
	// terms in the equivalence class and the term threshold.
	IDSet eqTerms = eq.terms();
	if (_termThreshold >= 0) {
	    if (eqTerms.length() > _termThreshold) {
		eqTerms = this.selectTerms(eqTerms, _termThreshold);
	    }
	    _terms.add(eqTerms);
	}
        for (int termId : eq.terms()) {
            _outTermNodeMap.println(termId + "\t" + eq.id());
        }
    }

    @Override
    public void open() {
        
    }
    
    /**
     * Select a subset of the given IDSet with size elements.
     * 
     * Selects elements at equi-distance.
     * 
     * @param terms
     * @param size
     * @return 
     */
    private IDSet selectTerms(IDSet values, int size) {
	
	List<Integer> candidates = values.toSortedList();
	
	double step = ((double)candidates.size())/ ((double)size);
	double pos = 0;
	
	HashIDSet result = new HashIDSet();
	for (int iElement = 0; iElement < size; iElement++) {
	    int index = (int)Math.floor(pos);
	    result.add(candidates.get(index));
	    pos += step;
	}
	
	return result;
    }
    
    public IDSet terms() {
	
	return _terms;
    }
}
