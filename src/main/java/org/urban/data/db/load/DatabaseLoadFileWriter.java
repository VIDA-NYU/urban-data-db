/*
 * Copyright 2018 New York University.
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
package org.urban.data.db.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.urban.data.core.object.filter.AnyObjectFilter;
import org.urban.data.core.object.filter.ObjectFilter;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.util.StringHelper;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.json.JsonQuery;
import org.urban.data.db.io.AdvancedEquivalenceClassReader;
import org.urban.data.db.io.TSVFileWriter;
import org.urban.data.db.io.TermIndexReader;

/**
 * Create all load files for a relational database containing information about
 * terms and equivalence classes.
 * 
 * Creates load files for the following tables:
 * 
 * term(id, value, type)
 * node(id)
 * term_node_map(term_id, node_id)
 * column_node_map(column_id, node_id)
 * node_containment(node_id1, node_id2) -> node 1 contains node 2
 * top_level_node(node_id)
 * 
 * The term threshold parameter allows to limit the number of terms per
 * equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseLoadFileWriter {
    
    private void createIndex(String tableName, String columns, PrintWriter out) {
    
        out.println("CREATE INDEX ON " + tableName + "(" + columns + ");");
    }
    
    private void dropTable(String tableName, PrintWriter out) {
    
        out.println("DROP TABLE IF EXISTS " + tableName + ";\n");
    }
    
    private void grantPrivileges(String tableName, String userName, PrintWriter out) {
        
        out.println("GRANT ALL PRIVILEGES ON TABLE " + tableName + " TO " + userName + ";\n");
    }
    
    private void loadFile(String tableName, String columns, File file, PrintWriter out) {
        
        out.println("\\copy " + tableName + "(" + columns + ") from './" + file.getName() + "' with delimiter E'\\t'\n");
    }
    
    private void primaryKey(String tableName, String columns, PrintWriter out) {
        
        out.println("ALTER TABLE " + tableName + " ADD PRIMARY KEY(" + columns + ");\n");
    }

    public void writeColumnsFile(
            File inputFile,
            String tableName,
            String userName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {
        
        if (inputFile.exists()) {
            FileSystem.createParentFolder(outputFile);
            JsonQuery engine = new JsonQuery(inputFile);
            String target = "columns";
            ArrayList<JQuery> select = new ArrayList<>();
            select.add(new JQuery("id"));
            select.add(new JQuery("dataset"));
            select.add(new JQuery("name"));
	    select.add(new JQuery("distinctValueCount"));
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String[] tuple : engine.query(target, select)) {
                    out.println(StringHelper.joinStrings(tuple, "\t"));
                }
            }
            this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  id INTEGER NOT NULL,");
            script.println("  name VARCHAR(255) NOT NULL,");
            script.println("  dataset CHAR(9) NOT NULL,");
            script.println("  term_count INTEGER NULL");
            script.println(");\n");
            this.loadFile(tableName, "id, dataset, name, term_count", outputFile, script);
            this.primaryKey(tableName, "id", script);
            this.grantPrivileges(tableName, userName, script);
       }
    }

    public void writeDatasetFile(
            File inputFile,
            String tableName,
            String userName,
            File outputFile,
            PrintWriter script) throws java.io.IOException {
        
        if (inputFile.exists()) {
            FileSystem.createParentFolder(outputFile);
            try (
                    BufferedReader in = FileSystem.openReader(inputFile);
                    PrintWriter out = FileSystem.openPrintWriter(outputFile)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (!line.equals("")) {
                        out.println(line);
                    }
                }
            }
            this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  id CHAR(9) NOT NULL,");
            script.println("  name VARCHAR(512) NOT NULL");
            script.println(");\n");
            this.loadFile(tableName, "id, name", outputFile, script);
            this.primaryKey(tableName, "id", script);
            this.grantPrivileges(tableName, userName, script);
        }
    }
    
    public void writeDomainFile(
            File inputFile,
            String tableName,
            String userName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        this.writeIdentifiableIDSetFile(
                inputFile,
                tableName,
                "domain_id",
                "node_id",
                userName,
                outputFile,
                script
        );
    }

    public void writeDatabaseDomainFile(
            File inputFile,
            String domainTableName,
            String mappingTableName,
            String userName,
            File outputDomainFile,
            File outputMappingFile,
            PrintWriter script
    ) throws java.io.IOException {

        if (inputFile.exists()) {
            // Domains
            FileSystem.createParentFolder(outputDomainFile);
	    new TSVFileWriter().convertIdentifiableIDSets(inputFile, outputDomainFile);
            this.dropTable(domainTableName, script);
            script.println("CREATE TABLE " + domainTableName + " (");
            script.println("  domain_id INTEGER NOT NULL,");
            script.println("  node_id INTEGER NOT NULL");
            script.println(");\n");
            this.loadFile(domainTableName, "domain_id, node_id", outputDomainFile, script);
            this.primaryKey(domainTableName, "domain_id, node_id", script);
            this.createIndex(domainTableName, "domain_id", script);
            this.createIndex(domainTableName, "node_id", script);
            script.println();
            this.grantPrivileges(domainTableName, userName, script);
            // Domains
            FileSystem.createParentFolder(outputDomainFile);
	    new TSVFileWriter().convertIdentifiableIDSets(inputFile, outputMappingFile, 2);
            this.dropTable(mappingTableName, script);
            script.println("CREATE TABLE " + mappingTableName + " (");
            script.println("  domain_id INTEGER NOT NULL,");
            script.println("  column_id INTEGER NOT NULL");
            script.println(");\n");
            this.loadFile(mappingTableName, "domain_id, column_id", outputMappingFile, script);
            this.primaryKey(mappingTableName, "domain_id, column_id", script);
            this.createIndex(mappingTableName, "domain_id", script);
            this.createIndex(mappingTableName, "column_id", script);
            script.println();
            this.grantPrivileges(mappingTableName, userName, script);
        }
    }

    private void writeIdentifiableIDSetFile(
            File inputFile,
            String tableName,
            String colName1,
            String colName2,
            String userName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        if (inputFile.exists()) {
            FileSystem.createParentFolder(outputFile);
	    new TSVFileWriter().convertIdentifiableIDSets(inputFile, outputFile);
	    this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  " + colName1 + " INTEGER NOT NULL,");
            script.println("  " + colName2 + " INTEGER NOT NULL");
            script.println(");\n");
            this.loadFile(tableName, colName1 + ", " + colName2, outputFile, script);
            this.primaryKey(tableName, colName1 + ", " + colName2, script);
            this.createIndex(tableName, colName1, script);
            this.createIndex(tableName, colName2, script);
            script.println();
            this.grantPrivileges(tableName, userName, script);
        }
    }

    public void writeRobustSignaturesFile(
            File inputFile,
            String tableName,
            String userName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        this.writeIdentifiableIDSetFile(
                inputFile,
                tableName,
                "node_id",
                "member_id",
                userName,
                outputFile,
                script
        );
    }

    public void writeTermsAndEquivalenceClasses(
            File eqFile,
            File termIndexFile,
            String termTableName,
            String termNodeMapTableName,
            String columnNodeMapTableName,
            String columnTermMapTableName,
            String userName,
            int termThreshold,
            File outputTermFile,
            File outputTermNodeMapFile,
            File outputColumnNodeMapFile,
            File outputColumnTermMapFile,
            PrintWriter script
    ) throws java.io.IOException {
        
        // Create parent folders for output files if they don't exist
        FileSystem.createParentFolder(outputTermFile);
        FileSystem.createParentFolder(outputTermNodeMapFile);
        FileSystem.createParentFolder(outputColumnNodeMapFile);
        FileSystem.createParentFolder(outputColumnTermMapFile);
        
        // Write equivalence classe mapping files. Get term filter if term
	// threshold is not negative.
	ObjectFilter termFilter;
        try (
            PrintWriter outColumnNodeMap = FileSystem.openPrintWriter(outputColumnNodeMapFile);
            PrintWriter outTermNodeMap = FileSystem.openPrintWriter(outputTermNodeMapFile)
        ) {
            EQFilesWriter eqWriter = new EQFilesWriter(
                    outColumnNodeMap,
                    outTermNodeMap,
                    termThreshold
            );
            new AdvancedEquivalenceClassReader().read(eqFile, eqWriter);
            if (termThreshold >= 0) {
                termFilter = eqWriter.terms();
            } else {
                termFilter = new AnyObjectFilter();
            }
        }

        //
        // Write term file
        //
        int maxTermLength = 0;
        try (
                PrintWriter outTerms = FileSystem.openPrintWriter(outputTermFile);
                PrintWriter outColumnTermMap = FileSystem.openPrintWriter(outputColumnTermMapFile)
        ) {
            TermFileWriter writer = new TermFileWriter(
                    termFilter,
                    outTerms,
                    outColumnTermMap
            );
            new TermIndexReader().read(termIndexFile, writer);
            maxTermLength = writer.maxLength();
        }
        
        System.out.println("LONGEST TERM IS " + maxTermLength);
        
        this.dropTable(termTableName, script);
        script.println("CREATE TABLE " + termTableName + "(");
        script.println("  id INTEGER NOT NULL,");
        script.println("  value VARCHAR(" + maxTermLength + ") NOT NULL,");
        script.println("  datatype INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(termTableName, "id, value, datatype", outputTermFile, script);
        this.primaryKey(termTableName, "id", script);
        script.println("ALTER TABLE " + termTableName + " ADD UNIQUE(value);\n");
        this.grantPrivileges(termTableName, userName, script);
        
        this.dropTable(termNodeMapTableName, script);
        script.println("CREATE TABLE " + termNodeMapTableName + "(");
        script.println("  term_id INTEGER NOT NULL,");
        script.println("  node_id INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(termNodeMapTableName, "term_id, node_id", outputTermNodeMapFile, script);
        this.primaryKey(termNodeMapTableName, "term_id", script);
        this.createIndex(termNodeMapTableName, "node_id", script);
        script.println();
        this.grantPrivileges(termNodeMapTableName, userName, script);
        
        this.dropTable(columnNodeMapTableName, script);
        script.println("CREATE TABLE " + columnNodeMapTableName + " (");
        script.println("  column_id INTEGER NOT NULL,");
        script.println("  node_id INTEGER NOT NULL,");
        script.println("  count INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(columnNodeMapTableName, "column_id, node_id, count", outputColumnNodeMapFile, script);
        this.primaryKey(columnNodeMapTableName, "column_id, node_id", script);
        this.createIndex(columnNodeMapTableName, "column_id", script);
        this.createIndex(columnNodeMapTableName, "node_id", script);
        script.println();
        this.grantPrivileges(columnNodeMapTableName, userName, script);
        
        this.dropTable(columnTermMapTableName, script);
        script.println("CREATE TABLE " + columnTermMapTableName + " (");
        script.println("  column_id INTEGER NOT NULL,");
        script.println("  term_id INTEGER NOT NULL,");
        script.println("  count INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(columnTermMapTableName, "column_id, term_id, count", outputColumnTermMapFile, script);
        this.primaryKey(columnTermMapTableName, "column_id, term_id", script);
        this.createIndex(columnTermMapTableName, "column_id", script);
        this.createIndex(columnTermMapTableName, "term_id", script);
        script.println();
        this.grantPrivileges(columnTermMapTableName, userName, script);
    }
}
