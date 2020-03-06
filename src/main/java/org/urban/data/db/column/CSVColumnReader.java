/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.column;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueCounterImpl;

/**
 * Read column files with frequency values that are stored as CSV files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CSVColumnReader extends ColumnReader {

    private final File _file;
    private final CSVFormat _format;
    private CSVParser _parser;
    private Iterator<CSVRecord> _records = null;
    private ValueCounter _value = null;
    
    public CSVColumnReader(File file, CSVFormat format, int columnId) {
        
        super(columnId);
        
        System.out.println(file.getAbsolutePath());
        
        _file = file;
        _format = format;
        this.reset();
    }
    
    public CSVColumnReader(File file, int columnId) {
        
        this(file, CSVFormat.TDF, columnId);
    }
    
    @Override
    public ColumnReader cloneReader() {

        return new CSVColumnReader(_file, _format, this.columnId());
    }

    @Override
    public void close() {

        try {
            _parser.close();
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean hasNext() {

        return (_value != null);
    }

    @Override
    public ValueCounter next() {

        ValueCounter result = _value;
        
        if (_records.hasNext()) {
            CSVRecord record = _records.next();
            _value = new ValueCounterImpl(
                    record.get(0),
                    Integer.parseInt(record.get(1))
            );
        } else {
            _value = null;
        }

        return result;
    }

    public ValueCounter peek() {
        
        return _value;
    }
    
    @Override
    public final void reset() {

        try {
            InputStream is = FileSystem.openFile(_file);
            _parser = new CSVParser(new InputStreamReader(is), _format);
            _records = _parser.iterator();
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        this.next();
    }
}
