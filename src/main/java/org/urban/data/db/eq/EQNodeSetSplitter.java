/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create a set of node identifier files for a given equivalence class file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQNodeSetSplitter {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <number-of-files>\n" +
            "  <output-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(EQNodeSetSplitter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int numberOfFiles = Integer.parseInt(args[1]);
        File outputDir = new File(args[2]);
        
        try {
            new EQIndex(eqFile)
                    .splitNodes(numberOfFiles, "nodes", outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
