/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

/**
 * Consumer for a stream of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQConsumer {
    
    public void close();
    public void consume(EQ node);
    public void open();
}
