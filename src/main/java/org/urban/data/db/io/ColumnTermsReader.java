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
package org.urban.data.db.io;

import java.io.File;
import org.urban.data.core.set.HashObjectSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.set.ImmutableIDSet;
import org.urban.data.core.set.ImmutableIdentifiableIDSet;
import org.urban.data.core.set.MutableIdentifiableIDSet;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.eq.AdvancedEquivalenceClass;
import org.urban.data.db.eq.EquivalenceClassConsumer;

/**
 * Read columns as sets of terms from an equivalence class file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnTermsReader {
    
    private class ColumnBuilder implements EquivalenceClassConsumer<AdvancedEquivalenceClass> {

        private HashObjectSet<MutableIdentifiableIDSet> _columns;

        @Override
        public void close() {

        }
        
        public IdentifiableObjectSet<ImmutableIdentifiableIDSet> columns() {
            
            HashObjectSet<ImmutableIdentifiableIDSet> result;
            result = new HashObjectSet<>();
            
            for (MutableIdentifiableIDSet column : _columns) {
                result.add(
                        new ImmutableIdentifiableIDSet(
                                column.id(),
                                new ImmutableIDSet(column.toSortedList(), true)
                        )
                );
            }
            
            return result;
        }

        @Override
        public void consume(AdvancedEquivalenceClass eq) {

            for (IdentifiableCount col : eq.columns()) {
                if (!_columns.contains(col.id())) {
                    _columns.add(
                            new MutableIdentifiableIDSet(col.id(), eq.terms())
                    );
                } else {
                    _columns.get(col.id()).add(eq.terms());
                }
            }
        }

        @Override
        public void open() {

            _columns = new HashObjectSet<>();
        }
    }
    
    private final File _file;
    
    public ColumnTermsReader(File file) {
        
        _file = file;
    }
    
    public IdentifiableObjectSet<ImmutableIdentifiableIDSet> read() throws java.io.IOException {
        
        ColumnBuilder consumer = new ColumnBuilder();
        new AdvancedEquivalenceClassReader(_file).read(consumer);
        return consumer.columns();
    }
}
