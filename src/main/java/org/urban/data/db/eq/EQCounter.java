/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

/**
 * Count number of equivalence classes in an equivalence class index.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQCounter implements EQConsumer {

    private int _count = 0;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(EQ node) {

        _count++;
    }

    public int equivalenceClassCount() {
        
        return _count;
    }
    
    @Override
    public void open() {

        _count = 0;
    }
}
