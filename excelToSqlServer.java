package org.example;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExcelToSqlServer {
    private static final String EXCEL_FILE = "8_june.xlsx";
    private static final String DB_URL = "jdbc:sqlserver://10.15.0.10;databaseName=central-sql-v2;encrypt=true;trustServerCertificate=true;";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "adp_k234@Rfgh11";
    private static final int CHUNK_SIZE = 1000;

    private static final Set<Integer> ALLOWED_SITE_IDS = new HashSet<>(Arrays.asList(
            47828,
            47565,
            47644,
            47810,
            47586,
            47851,
            47689,
            47824,
            47642,
            47802,
            47869,
            47640,
            47847,
            47835,
            47804,
            47759,
            47834,
            47827,
            47820,
            47809,
            47492,
            47688,
            47756,
            47845,
            47861,
            47807,
            47761,
            47755,
            47898,
            47678,
            47641,
            47762,
            47818,
            47870,
            47874,
            47493,
            47681,
            47564,
            47643,
            47532,
            47829,
            47832,
            47754,
            47516,
            47757,
            47813,
            47806,
            47894,
            47472,
            47800,
            47690,
            47819,
            47684,
            47825,
            47830,
            47457,
            47913,
            47853,
            47012,
            47213,
            47149,
            47079,
            47283,
            47203,
            47251,
            46997,
            47163,
            47038,
            47162,
            47121,
            46931,
            47795,
            47423,
            47174,
            46991,
            47161,
            47282,
            47502,
            47204,
            47517,
            47232,
            47659,
            47378,
            47226,
            47252,
            47186,
            47627,
            47658,
            47244,
            47273,
            47152,
            47227,
            47503,
            47119,
            47263,
            47178,
            47164,
            47177,
            47000,
            47515,
            47205,
            47421,
            47197,
            47253,
            47171,
            47347,
            47110,
            47153,
            47138,
            46998,
            47041,
            47211,
            47328,
            47117,
            47245,
            46999,
            47261,
            47250,
            47040,
            47217,
            47249,
            47318,
            47304,
            47246,
            47358,
            47151,
            47185,
            47768,
            44607,
            46471,
            47527,
            44940,
            46930,
            46049,
            44840,
            44795,
            47551,
            47471,
            44972,
            47390,
            47021,
            46448,
            46682,
            46544,
            46546,
            47093,
            45822,
            45200,
            44486,
            45307,
            45584,
            46426,
            46543,
            47080,
            45499,
            46923,
            47628,
            46545,
            47401,
            46547,
            47196,
            47193,
            47094,
            46472,
            47216,
            45057,
            47302,
            46628,
            46605,
            46936,
            44447,
            47384,
            45585,
            46743,
            46803,
            46800,
            44836,
            46953,
            45018,
            47037,
            46641,
            47081,
            47240,
            47499,
            47133,
            47006,
            47543,
            47382,
            47019,
            47667,
            47018,
            47180,
            47135,
            47179,
            47356,
            47148,
            47352,
            47181,
            47355,
            47354,
            47626,
            47764,
            47022,
            47443,
            47296,
            47297,
            47442,
            47182,
            47298,
            47558
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

        String insertSQL = "INSERT INTO SiteNetworkDailyReport " +
                "(report_date, siteid, pgid, vid, variation_type, sid, ntwid, ntauid, device_type, cid, ad_format_type, " +
                "total_requests, total_bids, total_timeouts, total_impressions, total_clicks, total_net_revenue, total_gross_revenue, " +
                "unique_impressions, unique_clicks, viewable_impressions, measurable_impressions) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                Date reportDate = null;
                Object reportDateObj = row.get("date");
                if (reportDateObj instanceof Date) {
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
                        reportDate,
                        castToInteger(row.get("site_id")),
                        castToInteger(row.get("pgid")),
                        castToInteger(row.get("vid")),
                        castToByte(row.get("variationtype")),
                        castToInteger(row.get("sid")),
                        castToInteger(row.get("ntwid")),
                        castToInteger(row.get("ntwauid")),
                        castToByte(row.get("device_type")),
                        castToInteger(row.get("cid")),
                        castToByte(row.get("ad_format_type")),
                        0, 0, 0,
                        castToInteger(row.get("ad_exchange_impressions")),
                        0,
                        castToDouble(row.get("net_revenue")),
                        castToDouble(row.get("ad_exchange_revenue")),
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
