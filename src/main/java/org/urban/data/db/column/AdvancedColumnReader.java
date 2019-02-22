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
package org.urban.data.db.column;

import java.io.File;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.db.io.AdvancedEquivalenceClassReader;

/**
 * Column reader using advanced equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AdvancedColumnReader {
    
    private final File _file;
    
    public AdvancedColumnReader(File file) {

        _file = file;
    }
    
    public IdentifiableObjectSet<AdvancedColumn> read() throws java.io.IOException {
        
        new AdvancedEquivalenceClassReader(_file);
        return null;
    }
}
