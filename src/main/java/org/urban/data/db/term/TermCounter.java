/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.term;

/**
 * Count number of terms in a term index.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermCounter implements TermConsumer {

    private int _count = 0;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(Term term) {

        _count++;
    }

    @Override
    public void open() {
        
        _count = 0;
    }
    
    public int termCount() {
        
        return _count;
    }
}
