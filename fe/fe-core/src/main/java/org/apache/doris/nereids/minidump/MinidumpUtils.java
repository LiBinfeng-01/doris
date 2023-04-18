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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Table;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util for minidump
 */
public class MinidumpUtils {

    public static String DUMP_PATH = null;

    /** Loading of minidump file */
    public static Minidump jsonMinidumpLoad(String dumpFilePath) throws IOException {
        // open file, read file, put them into minidump object
        try (FileInputStream inputStream = new FileInputStream(dumpFilePath + "/dumpFile.json")) {
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
            String catalogName = inputJSON.getString("CatalogName");
            String dbName = inputJSON.getString("DbName");
            JSONArray tablesJson = (JSONArray) inputJSON.get("Tables");
            for (int i = 0; i < tablesJson.length(); i++) {
                String tablePath = dumpFilePath + (String) tablesJson.get(i);
                DataInputStream dis = new DataInputStream(new FileInputStream(tablePath));
                Table newTable = Table.read(dis);
                tables.add(newTable);
            }
            String colocateTableIndexPath = dumpFilePath + inputJSON.getString("ColocateTableIndex");
            DataInputStream dis = new DataInputStream(new FileInputStream(colocateTableIndexPath));
            ColocateTableIndex newColocateTableIndex = new ColocateTableIndex();
            newColocateTableIndex.readFields(dis);

            JSONArray columnStats = (JSONArray) inputJSON.get("ColumnStatistics");
            Map<String, ColumnStatistic> columnStatisticMap = new HashMap<>();
            for (int i = 0; i < columnStats.length(); i++) {
                JSONObject oneColumnStat = (JSONObject) columnStats.get(i);
                String colName = oneColumnStat.keys().next();
                ColumnStatistic columnStatistic = ColumnStatistic.UNKNOWN;
                columnStatisticMap.put(colName, columnStatistic);
            }
            String parsedPlanJson = inputJSON.getString("ParsedPlan");
            String resultPlanJson = inputJSON.getString("ResultPlan");

            return new Minidump(sql, newSessionVariable, parsedPlanJson, resultPlanJson,
                tables, catalogName, dbName, columnStatisticMap, newColocateTableIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Minidump();
    }

    /** serialize tables from Table in catalog to json format */
    public static JSONArray serializeTables(
            String minidumpFileDir, String dbAndCatalogName, List<Table> tables) throws IOException {
        JSONArray tablesJson = new JSONArray();
        for (Table table : tables) {
            String tableFileName = dbAndCatalogName + table.getName();
            tablesJson.put(tableFileName);
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(minidumpFileDir + tableFileName));
            table.write(dos);
            dos.flush();
            dos.close();
        }
        return tablesJson;
    }

    public static void serializeColocateTableIndex(
            String colocateTableIndexFile, ColocateTableIndex colocateTableIndex) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(colocateTableIndexFile));
        colocateTableIndex.write(dos);
        dos.flush();
        dos.close();
    }

    /** serialize column statistic and replace when loading to dumpfile and environment */
    public static JSONArray serializeColumnStatistic(Map<String, ColumnStatistic> totalColumnStatisticMap) {
        JSONArray columnStatistics = new JSONArray();
        for (Map.Entry<String, ColumnStatistic> entry : totalColumnStatisticMap.entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            String colName = entry.getKey();
            JSONObject oneColumnStats = new JSONObject();
            oneColumnStats.put(colName, columnStatistic.toString());
            columnStatistics.put(oneColumnStats);
        }
        return columnStatistics;
    }

    /** init minidump utils before start to dump file, this will create a path */
    public static void init() {
        if (DUMP_PATH == null) {
            DUMP_PATH = System.getenv("DORIS_HOME") + "/log/minidump";
        }
        File minidumpDir = new File(DUMP_PATH);
        if (!minidumpDir.exists()) {
            minidumpDir.mkdirs();
        }
    }
}
