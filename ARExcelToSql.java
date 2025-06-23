package org.example;
import org.apache.poi.ss.usermodel.*;
        import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;
        import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class ArExcelToSqlServer {
    private static final String EXCEL_FILE = "ar_8_june.xlsx";
    private static final String DB_URL = "jdbc:sqlserver://10.15.0.10;databaseName=central-sql-v2;encrypt=true;trustServerCertificate=true;";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "pass";
    private static final int CHUNK_SIZE = 1000;

    private static final Set<Integer> ALLOWED_SITE_IDS = new HashSet<>(Arrays.asList(
          43084
    ));


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
            System.out.println(" All rows inserted successfully.");

        } catch (Exception e) {
            System.err.println(" Exception in main:");
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> readExcel(String filename) throws Exception {
        List<Map<String, Object>> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filename);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();


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

        String insertSQL = "INSERT INTO HistoricalArNetworkReportV2 " +
                "(ap_region, report_date, hour, siteid, cid, device_type, arpgid, arntwid, arntauid, " +
                "width, height, total_requests, total_impressions, total_clicks, total_revenue, " +
                "total_gross_revenue, total_net_revenue, " +
                "unique_impressions, unique_revenue, unique_gross_revenue, unique_net_revenue) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


        int successCount = 0;
        int failCount = 0;

        for (int rowIndex = 0; rowIndex < chunk.size(); rowIndex++) {
            Map<String, Object> row = chunk.get(rowIndex);

            Integer siteId = castToInteger(row.get("site_id"));
            if (!ALLOWED_SITE_IDS.contains(siteId)) {
                continue;
            }

            try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                // Parse report date
                java.sql.Date reportDate = null;
                Object reportDateObj = row.get("date");
                if (reportDateObj instanceof java.sql.Date) {
                    reportDate = (Date) reportDateObj;
                } else if (reportDateObj instanceof String) {
                    try {
                        String strDate = (String) reportDateObj;
                        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
                        java.util.Date parsed = sdf.parse(strDate);
                        reportDate = new java.sql.Date(parsed.getTime());
                    } catch (Exception e) {
                        System.err.println(" Invalid date format for report_date: " + reportDateObj);
                        throw e;
                    }
                }

                List<Object> values = Arrays.asList(
                        5,
                        reportDate,
                        23,
                        castToInteger(row.get("site_id")),
                        castToInteger(row.get("cid")),
                        castToByte(row.get("device_type")),
                        castToInteger(row.get("pgid")),
                        castToInteger(row.get("ntwid")),
                        castToInteger(row.get("ntwauid")),
                        0, 0, 0,
                        castToInteger(row.get("ad_exchange_impressions")),
                        0,
                        castToDouble(row.get("ad_exchange_revenue")),
                        castToDouble(row.get("ad_exchange_revenue")),
                        castToDouble(row.get("net_revenue")),
                        0, 0, 0, 0
                );

                setPreparedStatementParameters(stmt, values);
                stmt.executeUpdate();
                successCount++;

            } catch (Exception e) {
                failCount++;
                System.err.println(" Failed to insert row " + (rowIndex + 1));
                System.err.println("Row data: " + row);
                System.err.println("Reason: " + e.getMessage());
                // You can optionally log only FK errors by checking instance of SQLException or PSQLException
                continue;
            }
        }

        System.out.println("Summary:  " + successCount + " rows inserted,  " + failCount + " rows skipped.");
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

