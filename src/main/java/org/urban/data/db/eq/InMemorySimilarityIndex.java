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

import java.util.HashMap;


/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class InMemorySimilarityIndex extends EQSimilarityIndex {

    private final HashMap<Integer, HashMap<Integer, Integer>> _overlapIndex;
    
    public InMemorySimilarityIndex(Iterable<EQ> elements) {
        
        super(elements);
        
        _overlapIndex = new HashMap<>();
    }
    
    public synchronized void add(int eqId1, int eqId2, int overlap) {

        int lower, upper;
        if (eqId1 < eqId2) {
            lower = eqId1;
            upper = eqId2;
        } else if (eqId2 < eqId1) {
            lower = eqId2;
            upper = eqId1;
        } else {
            return;
        }
        
        HashMap<Integer, Integer> bucket;
        if (!_overlapIndex.containsKey(lower)) {
            bucket = new HashMap<>();
            _overlapIndex.put(lower, bucket);
        } else {
            bucket = _overlapIndex.get(lower);
        }
        bucket.put(upper, overlap);
    }
    
    @Override
    public int getOverlap(int eqId1, int eqId2) {

        int lower, upper;
        if (eqId1 < eqId2) {
            lower = eqId1;
            upper = eqId2;
        } else if (eqId2 < eqId1) {
            lower = eqId2;
            upper = eqId1;
        } else {
            return this.get(eqId1).columns().length();
        }
        
        if (_overlapIndex.containsKey(lower)) {
            HashMap<Integer, Integer> bucket = _overlapIndex.get(lower);
            if (bucket.containsKey(upper)) {
                return bucket.get(upper);
            }
        }
        return 0;
    }
}
