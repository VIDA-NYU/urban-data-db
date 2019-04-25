/*
 * Copyright 2019 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.urban.data.db.eq;

/**
 * Term in an  equivalence class. Contains the equivalence class and term
 * identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQTerm {
   
    private final int _nodeId;
    private final int _termId;
    
    public EQTerm(int nodeId, int termId) {
        
        _nodeId = nodeId;
        _termId = termId;
    }

    public EQTerm() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public int nodeId() {
        
        return _nodeId;
    }
    
    public int termId() {
        
        return _termId;
    }
}
