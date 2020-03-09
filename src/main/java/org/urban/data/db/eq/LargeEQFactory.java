/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

import org.urban.data.core.set.HashIDSet;
import org.urban.data.core.set.IDSet;

/**
 * Implementation of the factory pattern for large equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LargeEQFactory implements EQFactory {

    @Override
    public EQ getEQ(String line) {

        int tab1 = line.indexOf("\t");
        int tab2 = line.indexOf("\t", tab1 + 1);
        
        // Equivalence class identifier
        int id = Integer.parseInt(line.substring(0, tab1));
        // Count number of terms
        int termCount = 1;
        int pos = line.indexOf(",", tab1);
        while (pos != -1) {
            termCount++;
            pos = line.indexOf(",", pos + 1);
        }
        // Column list
        IDSet columns = new HashIDSet(line.substring(tab2 + 1).split(","));
        
        return new LargeEQ(id, columns, termCount);
    }
}
