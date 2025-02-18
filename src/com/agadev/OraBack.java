package com.agadev;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class OraBack {
    public static void main(String[] args) {

        String env = args[0].toString();

        // 실행 환경 (local, dev, prod)을 전달받음
        String environment = System.getProperty(env, "local"); // 기본값: local
        String configFileName = "config_" + environment + ".properties";
        String logFileName = "oraback_" + environment + ".log";

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
        String outputDir = config.getProperty("output.dir");

        try {
            // 백업 디렉토리 생성
            File dir = new File(outputDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // 데이터베이스 연결
            Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            LogUtil.log(logFileName, "Connected to Oracle Database in " + environment + " environment.");

            // 테이블 목록 가져오기
            Statement stmt = conn.createStatement();
            ResultSet tables = stmt.executeQuery("SELECT table_name FROM all_tables");

            while (tables.next()) {
                String tableName = tables.getString("table_name");
                LogUtil.log(logFileName, "Starting backup for table: " + tableName);

                try {
                    // 테이블 데이터 가져오기
                    Statement dataStmt = conn.createStatement();
                    ResultSet data = dataStmt.executeQuery("SELECT * FROM " + tableName);

                    // 데이터를 저장할 리스트 생성
                    List<Map<String, Object>> tableData = new ArrayList<>();

                    // 컬럼 메타데이터 가져오기
                    ResultSetMetaData metaData = data.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    // 데이터 행 처리
                    while (data.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object value = data.getObject(i);
                            row.put(columnName, value);
                        }
                        tableData.add(row);
                    }

                    // 리스트를 파일로 저장
                    File outputFile = new File(outputDir, tableName + ".txt");
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                        for (Map<String, Object> row : tableData) {
                            writer.write(row.toString());
                            writer.newLine();
                        }
                    }

                    dataStmt.close();
                    LogUtil.log(logFileName, "Backup completed for table: " + tableName);

                } catch (Exception e) {
                    LogUtil.log(logFileName, "Error backing up table: " + tableName + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }

            tables.close();
            stmt.close();
            conn.close();
            LogUtil.log(logFileName, "Backup completed successfully in " + environment + " environment.");
        } catch (Exception e) {
            LogUtil.log(logFileName, "Error during backup process: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
