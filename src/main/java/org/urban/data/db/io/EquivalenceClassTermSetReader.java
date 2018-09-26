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
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableIDSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.term.TermSet;

/**
 * Read only the set of terms for all equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EquivalenceClassTermSetReader {

    public IdentifiableObjectSet<TermSet> read(File file) throws java.io.IOException {

        HashObjectSet<TermSet> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                int eqId = Integer.parseInt(tokens[0]);
                result.add(new TermSet(eqId, new ImmutableIDSet(tokens[1])));
            }
        }
        
        return result;
    }
}
