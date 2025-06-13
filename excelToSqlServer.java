package org.example;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class ExcelToSqlServer {
    private static final String EXCEL_FILE = "9_june.xlsx";
    private static final String DB_URL = "jdbc:sqlserver://10.15.0.250;databaseName=central-sql-v2;encrypt=true;trustServerCertificate=true;";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";
    private static final int CHUNK_SIZE = 20000;

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            conn.setAutoCommit(false);
            IOUtils.setByteArrayMaxOverride(200_000_000);
            List<Map<String, Object>> rows = readExcel(EXCEL_FILE);

            System.out.println("Loaded " + rows.size() + " rows from Excel");

            int totalRows = rows.size();
            for (int i = 0; i < totalRows; i += CHUNK_SIZE) {
                int end = Math.min(i + CHUNK_SIZE, totalRows);
                List<Map<String, Object>> chunk = rows.subList(i, end);
                insertChunk(conn, chunk);
            }

            conn.commit();
            System.out.println("All rows inserted successfully.");

        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> readExcel(String filename) throws Exception {
        List<Map<String, Object>> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();

            // Read header
            Row headerRow = rowIterator.next();
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue());
            }

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Map<String, Object> rowData = new HashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    Object value = getCellValue(cell);
                    rowData.put(headers.get(i), value);
                }
                data.add(rowData);
            }
        }
        return data;
    }

    private static Object getCellValue(Cell cell) {
        switch (cell.getCellType()) {
            case STRING: return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue();
                } else {
                    return cell.getNumericCellValue();
                }
            case BOOLEAN: return cell.getBooleanCellValue();
            case BLANK: return null;
            default: return cell.toString();
        }
    }

    private static void insertChunk(Connection conn, List<Map<String, Object>> chunk) throws SQLException {
        System.out.println("Insert Start");
        String insertRecentSQL = "INSERT INTO SiteNetworkDailyReport " +
                "(report_date, siteid, cid, device_type, pgid, vid, variation_type, sid, ntwid, ntauid,  total_requests, total_impressions, " +
                "total_clicks, total_revenue, total_gross_revenue, total_net_revenue, unique_impressions, unique_revenue, unique_gross_revenue, unique_net_revenue) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        PreparedStatement recentStmt = conn.prepareStatement(insertRecentSQL);


        for (Map<String, Object> row : chunk) {
            Date reportDate = null;
            Object reportDateObj = row.get("report_date");
            if (reportDateObj instanceof Date) {
                reportDate = (Date) reportDateObj;
            } else if (reportDateObj instanceof String) {
                try {
                    reportDate = java.sql.Date.valueOf((String) reportDateObj);
                } catch (Exception e) {
                    System.err.println("Invalid date format for report_date: " + reportDateObj);
                }
            }

            java.sql.Timestamp dateCreated = new java.sql.Timestamp(System.currentTimeMillis()); // or from Excel if exists

            List<Object> values = Arrays.asList(
                    reportDate,
                    castToInteger(row.get("siteid")),
                    castToInteger(row.get("pgid")),
                    castToInteger(row.get("vid")),
                    castToByte(row.get("variation_type")),
                    castToInteger(row.get("sid")),
                    castToInteger(row.get("ntwid")),
                    castToInteger(row.get("ntauid")),
                    castToByte(row.get("device_type")),
                    castToInteger(row.get("cid")),
                    castToByte(row.get("ad_format_type")),
                    castToInteger(row.get("total_requests")),
                    0,
                    0,
                    castToInteger(row.get("total_impressions")),
                    0,
                    castToDouble(row.get("total_net_revenue")),
                    castToDouble(row.get("total_gross_revenue")),
                    0,
                    dateCreated,
                    0,
                    0,
                    0
            );

            System.out.println("Insert done");
                setPreparedStatementParameters(recentStmt, values);
                recentStmt.addBatch();

        }

        if (recentStmt != null) {
            int[] recentResults = recentStmt.executeBatch();
            System.out.println("Inserted " + recentResults.length + " rows into ArNetworkReportV2");
        }
        if (recentStmt != null) recentStmt.close();
    }

    private static void setPreparedStatementParameters(PreparedStatement stmt, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            Object val = values.get(i);
            if (val == null) {
                stmt.setNull(i + 1, Types.NULL);
            } else if (val instanceof Byte) {
                stmt.setByte(i + 1, (Byte) val);
            } else if (val instanceof Integer) {
                stmt.setInt(i + 1, (Integer) val);
            } else if (val instanceof Double) {
                stmt.setDouble(i + 1, (Double) val);
            } else if (val instanceof Float) {
                stmt.setFloat(i + 1, (Float) val);
            } else if (val instanceof String) {
                stmt.setString(i + 1, (String) val);
            } else {
                stmt.setObject(i + 1, val);
            }
        }
    }

    private static Byte castToByte(Object val) {
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).byteValue();
        try { return Byte.parseByte(val.toString()); } catch (Exception e) { return null; }
    }

    private static Integer castToInteger(Object val) {
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).intValue();
        try { return Integer.parseInt(val.toString()); } catch (Exception e) { return null; }
    }

    private static Double castToDouble(Object val) {
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try { return Double.parseDouble(val.toString()); } catch (Exception e) { return null; }
    }
}
