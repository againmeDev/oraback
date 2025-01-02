package com.agadev;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtil {

    // 로그 작성 메서드
    static void log(String logFileName, String message) {
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFileName, true))) {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            String logMessage = timestamp + " - " + message;
            // 콘솔 출력
            System.out.println(logMessage);
            
            logWriter.write(logMessage);
            logWriter.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + logFileName + " - " + e.getMessage());
        }
    }

}
