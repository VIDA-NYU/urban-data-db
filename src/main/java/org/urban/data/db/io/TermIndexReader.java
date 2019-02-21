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
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.set.EntitySet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.value.profiling.types.ValueTypeFactory;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.ColumnTermFilter;
import org.urban.data.db.term.TermConsumer;

/**
 * Read a term index file as a stream. Passes each term to a TermStreamHandler.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermIndexReader {
    
    private final File _file;
    
    public TermIndexReader(File file) {
	
	_file = file;
    }
    
    public void read(TermConsumer consumer) throws java.io.IOException {
        
        consumer.open();
        
        ValueTypeFactory types = new ValueTypeFactory();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                consumer.consume(
                        new ColumnTerm(
                                Integer.parseInt(tokens[0]),
                                tokens[1],
                                types.get(Integer.parseInt(tokens[2])),
                                tokens[3]
                        )
                );
            }
        }
        
        consumer.close();
    }
    
    public EntitySet readEntities(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        return new EntitySet(_file, filter);
    }
    
    public IdentifiableObjectSet<ColumnTerm> readTerms(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        ColumnTermFilter consumer = new ColumnTermFilter(filter);
        
        this.read(consumer);
        
        return consumer.terms();
    }
    
    public IdentifiableObjectSet<ColumnTerm> readTerms(File file) throws java.io.IOException {
        
        return this.readTerms(new AnyObjectFilter());
    }
}
