package com.ingestion.service;

import com.ingestion.model.JoinCondition;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ClickHouseService {
    private static final String URL = "jdbc:clickhouse://clickhouse:8123/default";
    private static final String USER = "default";
    private static final String PASSWORD = "";

    public Connection getConnection(String jwtToken) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        if (jwtToken != null && !jwtToken.isEmpty()) {
            properties.setProperty("password", jwtToken);
        } else if (!PASSWORD.isEmpty()) {
            properties.setProperty("password", PASSWORD);
        }
        try {
            Connection conn = DriverManager.getConnection(URL, properties);
            if (conn == null || conn.isClosed()) {
                throw new SQLException("Failed to establish ClickHouse connection");
            }
            return conn;
        } catch (SQLException e) {
            throw new SQLException("Cannot connect to ClickHouse at " + URL + ": " + e.getMessage(), e);
        }
    }

    // public List<String> getTables() throws SQLException {
    //     try (Connection conn = getConnection(null);
    //          Statement stmt = conn.createStatement();
    //          ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
    //         List<String> tables = new ArrayList<>();
    //         while (rs.next()) {
    //             tables.add(rs.getString(1));
    //         }
    //         return tables;
    //     }
    // }

    // public List<String> getColumns(String tableName) throws SQLException {
    //     try (Connection conn = getConnection(null);
    //          Statement stmt = conn.createStatement();
    //          ResultSet rs = stmt.executeQuery("DESCRIBE TABLE " + tableName)) {
    //         List<String> columns = new ArrayList<>();
    //         while (rs.next()) {
    //             columns.add(rs.getString("name"));
    //         }
    //         return columns;
    //     }
    // }

    public Map<String, List<String>> getColumnsForMultipleTables(List<String> tables) throws SQLException {
        Map<String, List<String>> result = new HashMap<>();
        for (String table : tables) {
            result.put(table, getColumns(table));
        }
        return result;
    }

    public String buildQuery(String mainTable, List<String> columns, List<JoinCondition> joinConditions) {
        StringBuilder query = new StringBuilder("SELECT ");
        
        // Handle column selection from multiple tables
        List<String> qualifiedColumns = columns.stream()
            .map(col -> {
                if (col.contains(".")) {
                    String[] parts = col.split("\\.");
                    return sanitize(parts[0]) + "." + sanitize(parts[1]);
                }
                return sanitize(mainTable) + "." + sanitize(col);
            })
            .collect(Collectors.toList());
        
        query.append(String.join(", ", qualifiedColumns))
             .append(" FROM ").append(sanitize(mainTable));
    
        for (JoinCondition jc : joinConditions) {
            query.append(" ")
                 .append(jc.getjoinType()).append(" JOIN ")
                 .append(sanitize(jc.getjoinTable()))
                 .append(" ON ")
                 .append(sanitize(jc.getMainTable())).append(".").append(sanitize(jc.getmaincolumn()))
                 .append(" = ")
                 .append(sanitize(jc.getjoinTable())).append(".").append(sanitize(jc.getjoincolumn()));
        }
        
        return query.toString();
    }
    
    private String sanitize(String identifier) {
        return identifier.matches("^[a-zA-Z0-9_]+$") ? 
               identifier : 
               "`" + identifier.replace("`", "``") + "`";
    }

    public long executeIngestion(String tableName, List<String> columns, String outputPath, 
                                List<JoinCondition> joinConditions) throws SQLException {
        String query = buildQuery(tableName, columns, joinConditions);
        try (Connection conn = getConnection(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            try (java.io.PrintWriter writer = new java.io.PrintWriter(outputPath)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                // Get column names from result set metadata
                List<String> outputColumns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    outputColumns.add(metaData.getColumnName(i));
                }
                
                // Write CSV header using actual column names from query result
                writer.println(String.join(",", outputColumns));
                
                long count = 0;
                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String value = rs.getString(i);
                        // Handle null values and escape special characters
                        if (value == null) {
                            values.add("");
                        } else {
                            // Escape quotes and commas
                            if (value.contains("\"") || value.contains(",")) {
                                value = "\"" + value.replace("\"", "\"\"") + "\"";
                            }
                            values.add(value);
                        }
                    }
                    writer.println(String.join(",", values));
                    count++;
                }
                return count;
            } catch (java.io.IOException e) {
                throw new SQLException("Failed to write to output file: " + e.getMessage(), e);
            }
        }
    }
    // In ClickHouseService.java
public List<String> getTables() throws SQLException {
    try (Connection conn = getConnection(null);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1));
        }
        return tables;
    }
}

public List<String> getColumns(String tableName) throws SQLException {
    try (Connection conn = getConnection(null);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT name FROM system.columns WHERE table = '" + tableName + "'"
         )) {
        List<String> columns = new ArrayList<>();
        while (rs.next()) {
            columns.add(rs.getString("name"));
        }
        return columns;
    }
}
}
