package com.agadev;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.*;
//import java.io.*;
//import java.sql.*;
//import java.util.*;

public class OraRestore {
    public static void main(String[] args) {
        // 실행 환경 (local, dev, prod)을 전달받음
        String environment = System.getProperty("env", "local"); // 기본값: local
        String configFileName = "config_" + environment + ".properties";
        String logFileName = "oraRestore_" + environment + ".log";

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

            // JSON 파일 디렉토리
            File dir = new File(srcDir);
            if (!dir.exists() || !dir.isDirectory()) {
                LogUtil.log(logFileName, "Input directory does not exist: " + srcDir);
                return;
            }

            // 디렉토리 내의 모든 JSON 파일 읽기
            //File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".json");
                }
            });

            if (files == null || files.length == 0) {
                LogUtil.log(logFileName, "No JSON files found in directory: " + srcDir);
                return;
            }

            for (File file : files) {
                String tableName = file.getName().replace(".json", "").toUpperCase();
                LogUtil.log(logFileName, "Processing table: " + tableName);

                // JSON 파일 읽기
                List<Map<String, Object>> dataList = readJsonFile(file, logFileName);
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

    // JSON 파일을 읽어서 List<Map<String, Object>> 반환
    private static List<Map<String, Object>> readJsonFile(File file, String logFileName) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(file, List.class);
        } catch (IOException e) {
            LogUtil.log(logFileName, "Error reading JSON file: " + file.getName());
            e.printStackTrace();
            return null;
        }
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

        // INSERT 쿼리 생성
		/*

        String insertQuery = buildInsertQuery(tableName, dataList.get(0).keySet());
        try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
            // 데이터 삽입
            for (Map<String, Object> row : dataList) {
                int paramIndex = 1;
                for (String column : row.keySet()) {
                    insertStmt.setObject(paramIndex++, row.get(column));
                }
//                System.out.println("insertStmt : " + insertStmt.get);
                
                insertStmt.addBatch();
            }

            // 배치 실행
            int[] result = insertStmt.executeBatch();
            System.out.println("Inserted " + result.length + " rows into table: " + tableName);
        }
		
		*/        
        
        try (Statement stmt = conn.createStatement()) {
            for (Map<String, Object> row : dataList) {
                // 완전 치환된 INSERT 쿼리 생성
                String insertQuery = buildInsertQueryWithValues(tableName, row);
                // 실행
                LogUtil.log(logFileName, insertQuery);
                stmt.executeUpdate(insertQuery);
            }

            LogUtil.log(logFileName, "Inserted " + dataList.size() + " rows into table: " + tableName);
        }
        
    }
    
    // INSERT 쿼리 생성 (값 포함)
    private static String buildInsertQueryWithValues(String tableName, Map<String, Object> row) {
        StringBuilder query = new StringBuilder();

        // 컬럼과 값 준비
        List<String> columns = new ArrayList<String>(row.keySet());
        List<String> values = new ArrayList<String>();

        int idx = 0;
        for (Object value : row.values()) {

        	String col = columns.get(idx).toString();
        	idx += 1;
        	if (value instanceof String) {
        		
        		switch (col) {
	        		case "HIRE_DATE":
	        			values.add("TO_DATE('" + value.toString() + "', 'YYYY-MM-DD HH24:MI:SS')");
	        			break;
        			default:
            			values.add("'" + value.toString().replace("'", "''") + "'"); // SQL 문자열 이스케이프
        				break;
        		}
                
            } else {
                values.add(value.toString());
            }
        	
        	
			/*        	

        		if (col.equals("HIRE_DATE")) {
        			values.add("TO_DATE('" + value.toString() + "', 'YYYY-MM-DD')");
        		}
        		else {
        			values.add("'" + value.toString().replace("'", "''") + "'"); // SQL 문자열 이스케이프
        		}
        	
        	if (value instanceof String) {
                values.add("'" + value.toString().replace("'", "''") + "'"); // SQL 문자열 이스케이프
            } else if (value instanceof java.util.Date) {
                values.add("TO_DATE('" + value.toString() + "', 'YYYY-MM-DD')");
            } else {
                values.add(value.toString());
            }
			*/        	
        }

        // INSERT INTO 생성
        query.append("INSERT INTO ").append(tableName).append(" (");
        query.append(joinWithDelimiter(", ", columns));
        query.append(") VALUES (");
        query.append(joinWithDelimiter(", ", values));
        query.append(")");

        return query.toString();
    }
    

    // INSERT 쿼리 생성
    
    private static String buildInsertQuery(String tableName, Set<String> columns) {
        StringBuilder query = new StringBuilder();

        // INSERT INTO 테이블명 추가
        query.append("INSERT INTO ").append(tableName).append(" (");

        // 컬럼명 연결
        query.append(joinWithDelimiter(", ", columns));

        query.append(") VALUES (");

        // ? 연결
        List<String> placeholders = Collections.nCopies(columns.size(), "?");
        query.append(joinWithDelimiter(", ", placeholders));

        query.append(")");
        return query.toString();
    }

    // Java 1.7용 문자열 연결 메서드
    private static String joinWithDelimiter(String delimiter, Iterable<String> elements) {
        StringBuilder sb = new StringBuilder();
        for (String element : elements) {
            if (sb.length() > 0) {
                sb.append(delimiter);
            }
            sb.append(element);
        }
        return sb.toString();
    }
    
    
	/*
	 * private static String buildInsertQuery(String tableName, Set<String> columns)
	 * { StringBuilder query = new StringBuilder();
	 * query.append("INSERT INTO ").append(tableName).append(" (");
	 * query.append(String.join(", ", columns)); query.append(") VALUES (");
	 * query.append(String.join(", ", Collections.nCopies(columns.size(), "?")));
	 * query.append(")"); return query.toString(); }
	 */
}
