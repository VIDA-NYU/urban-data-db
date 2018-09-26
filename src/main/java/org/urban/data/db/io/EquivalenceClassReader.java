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
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.db.eq.EquivalenceClass;
import org.urban.data.db.eq.EquivalenceClassConsumer;
import org.urban.data.db.eq.EquivalenceClassIndex;

/**
 * Read a term index file as a stream. Passes each term to a TermStreamHandler.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public abstract class EquivalenceClassReader<T extends EquivalenceClass> {
    
    public abstract T equivalenceClassFromString(String[] tokens);
    
    public void read(File file, EquivalenceClassConsumer<T> consumer, ObjectFilter filter) throws java.io.IOException {
        
        consumer.open();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                T eq = this.equivalenceClassFromString(tokens);
                if (filter.contains(eq.id())) {
                    consumer.consume(eq);
                }
            }
        }
        
        consumer.close();
    }

    public void read(File file, EquivalenceClassConsumer<T> consumer) throws java.io.IOException {
        
        this.read(file, consumer, new AnyObjectFilter());
    }
    
    public EquivalenceClassIndex<T> readIndex(File file) throws java.io.IOException {
        
        EquivalenceClassIndex<T> consumer = new EquivalenceClassIndex<>();
        this.read(file, consumer);
        return consumer;
    }
    
    public EquivalenceClassIndex<T> readIndex(File file, ObjectFilter filter) throws java.io.IOException {
        
        EquivalenceClassIndex<T> consumer = new EquivalenceClassIndex<>();
        this.read(file, consumer, filter);
        return consumer;
    }
}
