package com.agadev;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OraRestore {
    public static void main(String[] args) {

        String env = args[0];

        // 실행 환경 (local, dev, prod)을 전달받음
        String environment = System.getProperty(env, "local"); // 기본값: local
        String configFileName = "config_" + environment + ".properties";
        String logFileName = "oraRestore_" + environment + ".log";

        LogUtil.log(logFileName, configFileName);

        Properties config = new Properties();

        // 환경 파일 읽기
        try (InputStream input = new FileInputStream(configFileName)) {
            config.load(input);
        } catch (IOException e) {
            LogUtil.log(logFileName, "Error loading configuration file: " + configFileName);
            e.printStackTrace();
            return;
        }

        // 환경 변수 가져오기
        String jdbcUrl = config.getProperty("jdbc.url");
        String username = config.getProperty("jdbc.username");
        String password = config.getProperty("jdbc.password");
        String srcDir = config.getProperty("src.dir");

        try {
            // 데이터베이스 연결
            Connection conn = DriverManager.getConnection(jdbcUrl, username, password);

            // 텍스트 파일 디렉토리
            File dir = new File(srcDir);
            if (!dir.exists() || !dir.isDirectory()) {
                LogUtil.log(logFileName, "Input directory does not exist: " + srcDir);
                return;
            }

            // 디렉토리 내의 모든 텍스트 파일 읽기
            File[] files = dir.listFiles((d, name) -> name.endsWith(".txt"));

            if (files == null || files.length == 0) {
                LogUtil.log(logFileName, "No text files found in directory: " + srcDir);
                return;
            }

            for (File file : files) {
                String tableName = file.getName().replace(".txt", "").toUpperCase();
                LogUtil.log(logFileName, "Processing table: " + tableName);

                // 텍스트 파일 읽기
                List<Map<String, Object>> dataList = readTextFile(file, logFileName);
                if (dataList == null || dataList.isEmpty()) {
                    LogUtil.log(logFileName, "No data found in file: " + file.getName());
                    continue;
                }

                // DELETE + INSERT 작업 수행
                try {
                    performDeleteInsert(conn, tableName, dataList, logFileName);
                } catch (Exception e) {
                    LogUtil.log(logFileName, "Error processing table: " + tableName);
                    e.printStackTrace();
                }
            }

            conn.close();
            LogUtil.log(logFileName, "All files processed successfully.");

        } catch (Exception e) {
            LogUtil.log(logFileName, "Error connecting to database.");
            e.printStackTrace();
        }
    }

    // 텍스트 파일을 읽어서 List<Map<String, Object>> 반환
    private static List<Map<String, Object>> readTextFile(File file, String logFileName) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                Map<String, Object> row = parseTextLineToMap(line);
                dataList.add(row);
            }
        } catch (IOException e) {
            LogUtil.log(logFileName, "Error reading text file: " + file.getName());
            e.printStackTrace();
        }
        return dataList;
    }

    // 텍스트 줄을 Map<String, Object>로 변환
    private static Map<String, Object> parseTextLineToMap(String line) {
        Map<String, Object> map = new HashMap<>();
        line = line.substring(1, line.length() - 1); // 중괄호 제거
        String[] pairs = line.split(", ");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            String key = keyValue[0];
            String value = keyValue.length > 1 ? keyValue[1] : null;
            map.put(key, value);
        }
        return map;
    }

    // DELETE + INSERT 작업 수행
    private static void performDeleteInsert(Connection conn, String tableName, List<Map<String, Object>> dataList, String logFileName) throws SQLException {
        // DELETE 쿼리
        String deleteQuery = "DELETE FROM " + tableName;

        try (PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery)) {
            deleteStmt.executeUpdate();
            LogUtil.log(logFileName, "Deleted existing data from table: " + tableName);
        }

        if (dataList.isEmpty()) {
            LogUtil.log(logFileName, "No data to insert for table: " + tableName);
            return;
        }

        try (Statement stmt = conn.createStatement()) {
            for (Map<String, Object> row : dataList) {
                String insertQuery = buildInsertQueryWithValues(tableName, row);
                LogUtil.log(logFileName, insertQuery);
                stmt.executeUpdate(insertQuery);
            }

            LogUtil.log(logFileName, "Inserted " + dataList.size() + " rows into table: " + tableName);
        }
    }

    // INSERT 쿼리 생성 (값 포함)
    private static String buildInsertQueryWithValues(String tableName, Map<String, Object> row) {
        StringBuilder query = new StringBuilder();

        List<String> columns = new ArrayList<>(row.keySet());
        List<String> values = new ArrayList<>();

        for (String column : columns) {
            Object value = row.get(column);
            if (value == null || "null".equalsIgnoreCase(value.toString())) {
                values.add("NULL");
            } else if (value instanceof String) {
                values.add("'" + value.toString().replace("'", "''") + "'");
            } else {
                values.add(value.toString());
            }
        }

        query.append("INSERT INTO ").append(tableName).append(" (");
        query.append(String.join(", ", columns));
        query.append(") VALUES (");
        query.append(String.join(", ", values));
        query.append(")");

        return query.toString();
    }
}
