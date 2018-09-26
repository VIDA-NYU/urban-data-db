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
package org.urban.data.db.eq;

import org.urban.data.db.term.TermSetMeta;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.column.ColumnElement;

/**
 * An equivalence class is a set of terms.
 * 
 * There are various subclasses for different tasks. At its core the equivalence
 * class only adds the term count to the column element. Some subclasses will
 * maintain the list of terms that belong to the equivalence class. However,
 * this list can be quite large and is not necessary for all tasks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EquivalenceClass extends ColumnElement<IdentifiableCount>, TermSetMeta {
    
}
