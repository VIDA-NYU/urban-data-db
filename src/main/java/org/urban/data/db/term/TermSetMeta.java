/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.term;

import org.urban.data.core.object.IdentifiableObject;

/**
 * Meta data for a set of terms.
 * 
 * Contains the count of terms in the set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface TermSetMeta extends IdentifiableObject {
    
    /**
     * Number of terms in the equivalence class.
     * 
     * @return 
     */
    public int termCount();
}
