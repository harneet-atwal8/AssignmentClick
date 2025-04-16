package com.ingestion.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class FlatFileService {

    public List<String> getColumns(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + filePath);
        }
        try (CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            List<String> headers = new ArrayList<>(parser.getHeaderNames());
            if (headers.isEmpty()) {
                throw new IOException("CSV file has no headers");
            }
            // Validate headers
            for (String header : headers) {
                if (header == null || header.trim().isEmpty()) {
                    throw new IOException("CSV contains empty or null headers");
                }
            }
            return headers;
        } catch (Exception e) {
            throw new IOException("Failed to parse CSV headers: " + e.getMessage(), e);
        }
    }

    public long ingestToClickHouse(String filePath, List<String> columns, String tableName, 
                                   Connection clickHouseConn) throws IOException, SQLException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + filePath);
        }

        List<List<String>> data = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, 
                CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                List<String> row = new ArrayList<>();
                for (String col : columns) {
                    try {
                        row.add(record.get(col));
                    } catch (IllegalArgumentException e) {
                        throw new IOException("Column not found in CSV: " + col);
                    }
                }
                data.add(row);
            }
        } catch (Exception e) {
            throw new IOException("Failed to parse CSV data: " + e.getMessage(), e);
        }

        if (data.isEmpty()) {
            throw new IOException("CSV file contains no data rows");
        }

        // Sanitize column names for ClickHouse
        List<String> sanitizedColumns = columns.stream()
                .map(col -> {
                    if (col.matches("^[a-zA-Z0-9_]+$")) {
                        return col;
                    } else {
                        return "`" + col.replace("`", "``") + "`";
                    }
                })
                .collect(Collectors.toList());

        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
                tableName, sanitizedColumns.stream()
                        .map(c -> c + " String")
                        .collect(Collectors.joining(",")));
        try (PreparedStatement stmt = clickHouseConn.prepareStatement(createTableSql)) {
            stmt.execute();
        }

        String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, String.join(",", sanitizedColumns),
                columns.stream().map(c -> "?").collect(Collectors.joining(",")));
        try (PreparedStatement stmt = clickHouseConn.prepareStatement(insertSql)) {
            for (List<String> row : data) {
                for (int i = 0; i < row.size(); i++) {
                    stmt.setString(i + 1, row.get(i));
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
        }

        return data.size();
    }
}