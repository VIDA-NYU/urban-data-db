/*
 * Copyright 2018 Heiko Mueller <heiko.mueller@nyu.edu>.
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
package org.urban.data.db.io;

import java.io.File;
import java.io.PrintWriter;
import org.urban.data.core.io.IdentifiableIDSetReader;
import org.urban.data.core.set.IdentifiableIDSet;
import org.urban.data.core.set.IdentifiableObjectSet;
import org.urban.data.core.io.FileSystem;

/**
 * Convert input files into tab-delimited files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TSVFileWriter {
    
    /**
     * Convert a set of identifiable identifier sets in default file format into
     * a tab-delimited file.
     * 
     * The output file has two columns. The first column contains the set
     * identifier and the second column the set members.
     * 
     * @param inputFile
     * @param outputFile
     * @param listColumn
     * @throws java.io.IOException 
     */
    public void convertIdentifiableIDSets(
	    File inputFile,
	    File outputFile,
            int listColumn
    ) throws java.io.IOException {
	
	IdentifiableObjectSet<IdentifiableIDSet> sets;
	sets = new IdentifiableIDSetReader().read(inputFile, listColumn);
	try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
	    for (IdentifiableIDSet set : sets) {
		for (int nodeId : set) {
		    out.println(set.id() + "\t" + nodeId);
		}
	    }
	}
    }
    
    public void convertIdentifiableIDSets(
	    File inputFile,
	    File outputFile
    ) throws java.io.IOException {
        
        this.convertIdentifiableIDSets(inputFile, outputFile, 1);
    }
}
