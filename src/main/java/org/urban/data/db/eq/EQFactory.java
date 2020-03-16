/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

/**
 * Factory pattern to create equivalence class instances from a string
 * representation that is being read from disk.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQFactory {
    
    public EQ getEQ(String line);
}
