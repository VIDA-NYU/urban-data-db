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
package org.urban.data.db.eq;

import java.io.File;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.core.util.count.IdentifiableCounterSet;

/**
 * Index of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQIndex extends HashObjectSet<EQ> {

    public EQIndex(File eqFile) throws java.io.IOException {
        
        super(new EQReader(eqFile).read());
    }
    
    public int[] columnSizes() {
        
        IdentifiableCounterSet columns = new IdentifiableCounterSet();
        for (EQ node : this) {
            for (int columnId : node.columns()) {
                columns.inc(columnId, node.terms().length());
            }
        }
        
        int[] values = new int[columns.getMaxId() + 1];
        for (IdentifiableCount column : columns) {
            values[column.id()] = column.count();
        }
        return values;
    }
    
    public int[] nodeSizes() {
        
        int[] values = new int[this.getMaxId() + 1];
        for (EQ node : this) {
            values[node.id()] = node.terms().length();
        }
        return values;
    }
}
