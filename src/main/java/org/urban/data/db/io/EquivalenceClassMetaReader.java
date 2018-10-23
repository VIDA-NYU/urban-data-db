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
import org.apache.commons.lang3.StringUtils;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.term.TermSetMeta;
import org.urban.data.db.term.TermSetMetaImpl;

/**
 * Read meta data for a set of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EquivalenceClassMetaReader {

    public IdentifiableObjectSet<TermSetMeta> read(File file) throws java.io.IOException {

        HashObjectSet<TermSetMeta> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                int eqId = Integer.parseInt(tokens[0]);
                int termCount = StringUtils.countMatches(tokens[1], ",") + 1;
                result.add(new TermSetMetaImpl(eqId, termCount));
            }
        }
        
        return result;
    }
}
