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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.FileReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.set.HashIDSet;

/**
 * Print information about database column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnInfoPrinter {
   
    private final static String COMMAND =
            "Usage:\n" +
            "  <column-file>\n" +
            "  <column-id{s}>";

    private final static Logger LOGGER = Logger.getGlobal();
    

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        HashIDSet columnIds = new HashIDSet();
        for (String colId : args[1].split(",")) {
            columnIds.add(Integer.parseInt(colId));
        }
        
        JsonArray columns = null;
        try {
            columns = new JsonParser()
                    .parse(new FileReader(inputFile))
                    .getAsJsonObject()
                    .get("columns")
                    .getAsJsonArray();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ COLUMNS FILE", ex);
            System.exit(-1);
        }
        
        for (int iColumn = 0; iColumn < columns.size(); iColumn++) {
            JsonObject col = columns.get(iColumn).getAsJsonObject();
            if (columnIds.contains(col.get("id").getAsInt())) {
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                System.out.println(gson.toJson(col));
            }
        }
    }
}
