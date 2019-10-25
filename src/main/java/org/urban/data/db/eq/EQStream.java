/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

/**
 * Stream of equivalence classes. The equivalence classes in the stream are
 * output to a given consumer for processing. Allows to build pipelines for
 * equivalence class processing.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQStream {
    
    public void stream(EQConsumer consumer);
}
