// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.minidump;

import org.apache.doris.catalog.Table;
import org.apache.doris.qe.SessionVariable;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Util for minidump
 */
public class MinidumpUtils {

    /** Loading of minidump file */
    public static Minidump jsonMinidumpLoad(String path, String dumpFilePath) {
        // open file, read file, put them into minidump object
        try (FileInputStream inputStream = new FileInputStream(dumpFilePath)) {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = inputStream.read()) != -1) {
                sb.append((char) ch);
            }
            String inputString = sb.toString();
            // Parse the JSON string back into a JSON object
            JSONObject inputJSON = new JSONObject(inputString);
            SessionVariable newSessionVariable = new SessionVariable();
            newSessionVariable.readFromJson(inputJSON.getString("SessionVariable"));
            String sql = inputJSON.getString("Sql");

            List<Table> tables = new ArrayList<>();
            JSONArray tablesJson = (JSONArray) inputJSON.get("Tables");
            for (int i = 0; i < tablesJson.length(); i++) {
                String tablePath = path + (String) tablesJson.get(i);
                DataInputStream dis = new DataInputStream(new FileInputStream(tablePath));
                Table newTable = Table.read(dis);
                tables.add(newTable);
            }
            String parsedPlanJson = inputJSON.getString("ParsedPlan");
            String resultPlanJson = inputJSON.getString("ResultPlan");

            return new Minidump(sql, newSessionVariable, parsedPlanJson, resultPlanJson, tables);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Minidump();
    }

    public static String generateMinidumpFileName(String minidumpName) {
        return minidumpName;
    }
}
