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

import java.io.BufferedReader;
import java.io.File;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;

/**
 * Read a set of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQReader {
   
    private final File _file;
    
    public EQReader(File file) {
        
        _file = file;
    }
    
    public IdentifiableObjectSet<EQ> read() throws java.io.IOException {
        
        HashObjectSet<EQ> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    result.add(new EQImpl(line.split("\t")));
                }
            }
        }
        
        return result;
    }
}
