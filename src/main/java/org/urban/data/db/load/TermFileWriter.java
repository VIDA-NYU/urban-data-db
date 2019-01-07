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
package org.urban.data.db.load;

import java.io.PrintWriter;
import org.apache.commons.lang.StringEscapeUtils;
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.util.count.IdentifiableCount;
import org.urban.data.db.term.ColumnTerm;
import org.urban.data.db.term.TermConsumer;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermFileWriter implements TermConsumer {

    private static final int MAX_VALUE_LENGTH = 2048;
    
    private final ObjectFilter _filter;
    private int _maxLength = 0;
    private final PrintWriter _outTerm;
    private final PrintWriter _outColumntermMap;
    private int _trunc_count = 0;
    
    public TermFileWriter(
            ObjectFilter filter,
            PrintWriter outTerm,
            PrintWriter outColumntermMap
    ) {
	_filter = filter;
        _outTerm = outTerm;
        _outColumntermMap = outColumntermMap;
    }

    public TermFileWriter(
            PrintWriter outTerm,
            PrintWriter outColumntermMap
    ) {
	
	this(new AnyObjectFilter(), outTerm, outColumntermMap);
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ColumnTerm term) {
        
        for (IdentifiableCount column : term.columns()) {
            _outColumntermMap.println(
                    column.id() + "\t" +
                    term.id() + "\t" +
                    column.count()
            );
        }

        if (!_filter.contains(term.id())) {
	    return;
	}
	
        String value = term.value();
        
        value = StringEscapeUtils.escapeSql(value);
        if (value.contains("\\")) {
            value = value.replaceAll("\\\\", "\\\\\\\\");
        }
        if (value.length() > MAX_VALUE_LENGTH) {
            value = value.substring(0, MAX_VALUE_LENGTH) + "__" + (_trunc_count++);
        }
        if (value.length() > _maxLength) {
            _maxLength = value.length();
        }
        _outTerm.println(term.id() + "\t" + value + "\t" + term.type().id());
    }

    public int maxLength() {
        
        return _maxLength;
    }
    
    @Override
    public void open() {

        _trunc_count = 0;
    }
}
