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
import org.urban.data.core.set.IDSet;
import org.urban.data.core.set.ImmutableObjectSet;

/**
 * Equivalence class with a single representative term.
 * 
 * This type of equivalence class is intended for cases where we only need to
 * print one representative term for each equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EnhancedEquivalenceClass extends ImmutableEquivalenceClass {

    private final int _termId;
    
    public EnhancedEquivalenceClass(
            int id,
            IDSet terms,
            ImmutableObjectSet<IdentifiableCount> columns
    ) {

	super(id, terms.length(), columns);
	
	_termId = terms.first();
    }
    
    public EnhancedEquivalenceClass(
            int id,
            IDSet terms,
            String columns
    ) {

	super(id, terms.length(), columns);
	
	_termId = terms.first();
    }
    
    public int termId() {
	
	return _termId;
    }
}
