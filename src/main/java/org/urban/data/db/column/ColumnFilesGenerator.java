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
package org.urban.data.db.column;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;
import org.urban.data.core.value.ValueSet;
import org.urban.data.core.util.count.Counter;
import org.urban.data.core.value.AnySetFilter;
import org.urban.data.core.value.ShortValueSetFilter;
import org.urban.data.core.value.TextValueSetFilter;
import org.urban.data.core.value.ValueSetFilter;
import org.urban.data.core.io.FileSystem;

/**
 * Create dataset column files.
 * 
 * Parses a directory of dataset files. Transforms each CSV or TSV file in
 * the input directory into a set of files, one for each column in the dataset.
 * Considers any file with suffix .csv, .csv.gz, .tsv, or .tsv.gz as input
 * dataset files.
 * 
 * Assumes that the first row of each dataset file contains the column names.
 * 
 * Output files are numbered 0 to n. The file number corresponds to the unique
 * column identifier that will be used by other programs in the D6 workflow.
 * 
 * Information about individual columns is written to an output file. The file
 * is in JSON format and contains one record per database column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesGenerator {
    
    private final static Logger LOGGER = Logger.getLogger(ColumnFilesGenerator.class.getName());
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-directory>\n" +
            "  <columns-file>\n" +
            "  <to-upper>\n" +
            "  <max-value-length> [-1 to ignore]\n" +
            "  <max-value-length-fraction-threshold>\n" +
            "  <text-column-only>\n" +
            "  <text-fraction-threshold>\n" +
            "  <output-directory>";

    public void run(
            File inputDir,
            boolean toUpper,
            boolean removePunctuation,
            ValueSetFilter filter,
            File columnsFile,
            File outputDir
    ) throws java.io.IOException {
        
        // Create output directory if it does not exist.
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        
	// Get the first column's ID value
	Counter columnId = new Counter(0);

        ArrayList<File> files = new ArrayList<>();
        for (File file : inputDir.listFiles()) {
            if (
                    (file.getName().endsWith(".tsv")) || 
                    (file.getName().endsWith(".tsv.gz")) || 
                    (file.getName().endsWith(".csv")) || 
                    (file.getName().endsWith(".csv.gz"))
            ) {
                files.add(file);
            }
        }
        Collections.sort(files, new Comparator<File>(){
            @Override
            public int compare(File f1, File f2) {
                return Long.compare(f1.length(), f2.length());
            }
        });
        Collections.reverse(files);
        
	/*
	 * Iterate through files in input directory. Each file ending with
	 * .tsv or .tsv.gz is expected to be a dataset in TSV format. Files
	 * ending with .tsv.gz are expected to be zipped files.
	 */
        int columnsProcessed = 0;
        int columnsWritten = 0;
        long totalRowCount = 0;
        try (JsonWriter out = FileSystem.openJsonWriter(columnsFile)) {
            Gson gson = new Gson();
            out.beginObject().name("columns").beginArray();
            for (File file : files) {
                System.out.println(file.getName());
                CSVFormat format;
                if (((file.getName().endsWith(".tsv"))) || (file.getName().endsWith(".tsv.gz"))) {
                    format = CSVFormat.TDF;
                } else {
                    format = CSVFormat.DEFAULT;
                }
                try (CSVParser in = new CSVParser(new InputStreamReader(FileSystem.openFile(file)), format)) {
                    Iterator<CSVRecord> records = in.iterator();
                    CSVRecord headline = records.next();
                    ValueSet[] columns = new ValueSet[headline.size()];
                    for (int iColumn = 0; iColumn < columns.length; iColumn++) {
                        columns[iColumn] = new ValueSet();
                    }
                    int lineCount = 1;
                    while (records.hasNext()) {
                        CSVRecord row = records.next();
                        if (row.size() != headline.size()) {
                            System.out.println("Invalid file format in line " + lineCount);
                            System.out.println("Expected " + headline.size() + " columns, found " + row.size());
                            break;
                        }
                        for (int iColumn = 0; iColumn < headline.size(); iColumn++) {
                            String value = row.get(iColumn).trim();
                            if (toUpper) {
                                value = value.toUpperCase();
                            }
                            if (removePunctuation) {
                                value = value.replaceAll("[.,;]", "");
                            } else if (value.endsWith(".")) {
                                // Remove trailing period if it is the only
                                // occurrence in the value
                                if (value.indexOf(".") == value.length() - 1) {
                                    value = value.substring(0, value.length() - 1);
                                }
                            }
                            if (value.length() > 0) {
                                columns[iColumn].add(value);
                            }
                        }
                        lineCount++;
                    }
                    totalRowCount += (lineCount - 1);
                    columnsProcessed += headline.size();
                    for (int iColumn = 0; iColumn < headline.size(); iColumn++) {
                        ValueSet column = columns[iColumn];
                        if ((!column.isEmpty()) && (filter.accept(column))) {
                            JsonObject colInfo = this.writeColumnFile(
                                file,
                                headline.get(iColumn),
                                iColumn,
                                column,
                                columnId,
                                outputDir
                            );
                            gson.toJson(colInfo, out);
                            columnsWritten++;
                        }
                    }
                } catch (java.io.IOException exIn) {
                    LOGGER.log(Level.SEVERE, file.getName(), exIn);
                    System.exit(-1);
                }
            }
            out.endArray().endObject();
        }
        System.out.println("TOTAL FILES PROCESSED: " + files.size());
        System.out.println("TOTAL ROWS READ: " + totalRowCount);
        System.out.println("TOTAL COLUMNS PROCESSED: " + columnsProcessed);
        System.out.println("TOTAL FILES WRITTERN: " + columnsWritten);        
    }

    private JsonObject writeColumnFile(
        File inputFile,
        String columnName,
        int columnIndex,
        ValueSet values,
        Counter columnId,
        File outputDir
    ) throws java.io.IOException {
	
        JsonObject result = new JsonObject();
        
        String name = Normalizer.normalize(
               columnName,
                Normalizer.Form.NFKD
        );
        name = name.replaceAll("[^\\x00-\\x7F]", "");
        name = name.replaceAll("\\?", "");
        name = name.replaceAll("\\s", "_");
        
	String datasetId = inputFile.getName().split("\\.")[0];
        
        int totalCount;
        
        String columnFileName = columnId.value() + ".txt";
        File outputFile = new File(outputDir.getAbsolutePath() + File.separator + columnFileName);
        try (PrintWriter colOut = FileSystem.openPrintWriter(outputFile)) {
            totalCount = values.write(colOut);
        }

        JsonObject inFileInfo = new JsonObject();
        inFileInfo.add("fileName", new JsonPrimitive(inputFile.getName()));
        inFileInfo.add("columnHeader", new JsonPrimitive(StringEscapeUtils.escapeJson(columnName)));
        inFileInfo.add("columnIndex", new JsonPrimitive(columnIndex));
        
        JsonObject outFileInfo = new JsonObject();
        outFileInfo.add("fileName", new JsonPrimitive(columnFileName));
        
        JsonObject files = new JsonObject();
        files.add("input", inFileInfo);
        files.add("output", outFileInfo);
        
        result.add("id", new JsonPrimitive(columnId.value()));
        result.add("dataset", new JsonPrimitive(datasetId));
        result.add("name", new JsonPrimitive(name));
        result.add("totalValueCount", new JsonPrimitive(totalCount));
        result.add("distinctValueCount", new JsonPrimitive(values.size()));
        result.add("files", files);
        
	columnId.inc();
        
        return result;
    }

    public static void main(String[] args) {
	
	if (args.length != 9) {
	    System.out.println(COMMAND);
            System.out.println("Given arguments:");
            for (int iArg = 0; iArg < args.length; iArg++) {
                System.out.println("  " + iArg + ": " + args[iArg]);
            }
	    System.exit(-1);
	}
	
	File inputDir = new File(args[0]);
        File columnsFile = new File(args[1]);
	boolean toUpper = Boolean.parseBoolean(args[2]);
	boolean removePunctuation = Boolean.parseBoolean(args[3]);
        int maxValueLength = Integer.parseInt(args[4]);
        double lengthFilterThreshold = Double.parseDouble(args[5]);
        boolean textOnly = Boolean.parseBoolean(args[6]);
        double textFilterThreshold = Double.parseDouble(args[7]);
	File outputDir =  new File(args[8]);

        ValueSetFilter filter;
        if (textOnly) {
            filter = new TextValueSetFilter(textFilterThreshold);
        } else {
            filter = new AnySetFilter();
        }
        if (maxValueLength > 0) {
            filter = new ShortValueSetFilter(
                    maxValueLength,
                    lengthFilterThreshold,
                    filter
            );
        }
        
        try {
            new ColumnFilesGenerator().run(
                    inputDir,
                    toUpper,
                    removePunctuation,
                    filter,
                    columnsFile,
                    outputDir
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "CREATE COLUMN FILES", ex);
            System.exit(-1);
        }
    }
}
