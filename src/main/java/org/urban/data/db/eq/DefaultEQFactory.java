/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.db.eq;

/**
 * Default implementation of the EQ factory pattern.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DefaultEQFactory implements EQFactory {

    @Override
    public EQ getEQ(String line) {

        return new EQImpl(line.split("\t"));
    }
}
