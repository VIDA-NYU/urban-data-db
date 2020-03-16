/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

import org.urban.data.core.object.IdentifiableObjectImpl;
import org.urban.data.core.set.IDSet;

/**
 * Implementation of the equivalence class interface for large collections of
 * terms. DOES NOT maintain the list of term identifier!.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LargeEQ extends IdentifiableObjectImpl implements EQ {

    private final IDSet _columns;
    private final int _termCount;
    
    public LargeEQ(int id, IDSet columns, int termCount) {
        
        super(id);
        
        _columns = columns;
        _termCount = termCount;
    }
    @Override
    public int columnCount() {
    
        return _columns.length();
    }

    @Override
    public IDSet columns() {

        return _columns;
    }

    @Override
    public int termCount() {

        return _termCount;
    }

    @Override
    public IDSet terms() {
        
        throw new UnsupportedOperationException("Not supported for large EQs.");
    }
}
