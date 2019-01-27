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

import java.io.File;
import org.urban.data.core.set.HashIDSet;
import org.urban.data.db.eq.EnhancedEquivalenceClass;

/**
 * Reader for enhanced equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EnhancedEquivalenceClassReader extends EquivalenceClassReader<EnhancedEquivalenceClass> {

    public EnhancedEquivalenceClassReader(File file) {
	
	super(file);
    }
    
    @Override
    public EnhancedEquivalenceClass equivalenceClassFromString(String[] tokens) {

        return new EnhancedEquivalenceClass(
                Integer.parseInt(tokens[0]),
                new HashIDSet(tokens[1].split(",")),
                tokens[2]
        );
    }
}
