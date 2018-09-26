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

import org.urban.data.db.eq.EquivalenceClass;
import org.urban.data.db.eq.ImmutableEquivalenceClass;

/**
 * Default reader for equivalence classes from compressed term index file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DefaultEquivalenceClassReader extends EquivalenceClassReader<EquivalenceClass> {

    @Override
    public EquivalenceClass equivalenceClassFromString(String[] tokens) {

        return new ImmutableEquivalenceClass(
                Integer.parseInt(tokens[0]),
                tokens[1].split(",").length,
                tokens[2]
        );
    }
}
