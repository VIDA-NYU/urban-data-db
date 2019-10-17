/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

import java.io.File;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.util.MemUsagePrinter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimIndex {
    
    public void run(File eqFile, int threads) throws java.io.IOException {
        
        System.out.println("START @ " + new Date());
        EQSimilarityIndex sim = new EQIndex(eqFile).computeOverlap(threads);
        System.out.println("DONE @ " + new Date());
        new MemUsagePrinter().print();
        System.out.println(sim.length() + " EQUIVALENCE CLASSES");
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>";
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        
        try {
            new SimIndex().run(eqFile, threads);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
        }
    }
}
