package com.ingestion.controller;

import com.ingestion.model.IngestionRequest;
import com.ingestion.model.JoinCondition;
import com.ingestion.service.ClickHouseService;
import com.ingestion.service.FlatFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@RestController
@RequestMapping("/ingestion")
public class IngestionController {
    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private FlatFileService flatFileService;

    @GetMapping("/tables")
    public ResponseEntity<?> getTables() {
        try {
            return ResponseEntity.ok(clickHouseService.getTables());
        } catch (SQLException e) {
            return ResponseEntity.badRequest().body("Error fetching tables: " + e.getMessage());
        }
    }

    @GetMapping("/columns")
    public ResponseEntity<?> getColumns(@RequestParam String source,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false) String filePath) {
        try {
            if ("clickhouse".equals(source)) {
                if (tableName == null || tableName.isEmpty()) {
                    return ResponseEntity.badRequest().body("Table name is required for ClickHouse source");
                }
                return ResponseEntity.ok(clickHouseService.getColumns(tableName));
            } else if ("flatfile".equals(source)) {
                if (filePath == null || filePath.isEmpty()) {
                    return ResponseEntity.badRequest().body("File path is required for Flat File source");
                }
                File file = new File(filePath);
                if (!file.exists()) {
                    return ResponseEntity.badRequest().body("File does not exist: " + filePath);
                }
                return ResponseEntity.ok(flatFileService.getColumns(filePath));
            }
            return ResponseEntity.badRequest().body("Invalid source");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error fetching columns: " + e.getMessage());
        }
    }

    @PostMapping("/columns/multiple")
    public ResponseEntity<?> getColumnsForMultipleTables(@RequestBody List<String> tables) {
        try {
            return ResponseEntity.ok(clickHouseService.getColumnsForMultipleTables(tables));
        } catch (SQLException e) {
            return ResponseEntity.badRequest().body("Error fetching columns: " + e.getMessage());
        }
    }

    @PostMapping("/start")
    public ResponseEntity<?> startIngestion(@RequestBody IngestionRequest request) {
        try {
            long recordCount;
            if ("clickhouse".equals(request.getSource())) {
                if (request.getTableName() == null || request.getTableName().isEmpty()) {
                    return ResponseEntity.badRequest().body("Table name is required");
                }
                if (request.getFilePath() == null || request.getFilePath().isEmpty()) {
                    return ResponseEntity.badRequest().body("Output file path is required");
                }
                String outputFileName = new File(request.getFilePath()).getName();
                String outputPath = "/app/uploads/" + outputFileName;
                // Convert List<Map<String, String>> to List<JoinCondition>
                List<JoinCondition> joinConditions = convertToJoinConditions(request.getJoinConditions());
                recordCount = clickHouseService.executeIngestion(
                        request.getTableName(), request.getColumns(),
                        outputPath, joinConditions);
            } else if ("flatfile".equals(request.getSource())) {
                if (request.getFilePath() == null || request.getFilePath().isEmpty()) {
                    return ResponseEntity.badRequest().body("Input file path is required");
                }
                if (request.getTableName() == null || request.getTableName().isEmpty()) {
                    return ResponseEntity.badRequest().body("Target table name is required");
                }
                File file = new File(request.getFilePath());
                if (!file.exists()) {
                    return ResponseEntity.badRequest().body("Input file does not exist: " + request.getFilePath());
                }
                try (java.sql.Connection conn = clickHouseService.getConnection(request.getJwtToken())) {
                    recordCount = flatFileService.ingestToClickHouse(
                            request.getFilePath(), request.getColumns(), request.getTableName(), conn);
                }
            } else {
                return ResponseEntity.badRequest().body("Invalid source");
            }
            return ResponseEntity.ok("Ingestion completed. Records processed: " + recordCount);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Ingestion failed: " + e.getMessage());
        }
    }

    @PostMapping("/upload")
    public ResponseEntity<?> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            if (file == null || file.isEmpty()) {
                return ResponseEntity.badRequest().body("No file uploaded");
            }
            File uploadDir = new File("/app/uploads");
            if (!uploadDir.exists()) {
                if (!uploadDir.mkdirs()) {
                    return ResponseEntity.badRequest().body("Failed to create uploads directory");
                }
            }
            String fileName = file.getOriginalFilename();
            if (fileName == null || fileName.isEmpty()) {
                return ResponseEntity.badRequest().body("Invalid file name");
            }
            File destFile = new File(uploadDir, fileName);
            file.transferTo(destFile);
            return ResponseEntity.ok(destFile.getAbsolutePath());
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("File upload failed: " + e.getMessage());
        }
    }

    @PostMapping("/preview")
    public ResponseEntity<?> previewData(@RequestBody IngestionRequest request) {
        try {
            if ("clickhouse".equals(request.getSource())) {
                // Convert List<Map<String, String>> to List<JoinCondition>
                List<JoinCondition> joinConditions = convertToJoinConditions(request.getJoinConditions());
                String query = clickHouseService.buildQuery(
                        request.getTableName(), request.getColumns(), joinConditions) + " LIMIT 100";
                try (java.sql.Connection conn = clickHouseService.getConnection(request.getJwtToken());
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(query)) {
                    List<Map<String, Object>> rows = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (String col : request.getColumns()) {
                            row.put(col, rs.getObject(col));
                        }
                        rows.add(row);
                    }
                    return ResponseEntity.ok(rows);
                }
            } else if ("flatfile".equals(request.getSource())) {
                if (request.getFilePath() == null || request.getFilePath().isEmpty()) {
                    return ResponseEntity.badRequest().body("File path is required");
                }
                File file = new File(request.getFilePath());
                if (!file.exists()) {
                    return ResponseEntity.badRequest().body("File does not exist: " + request.getFilePath());
                }
                List<Map<String, String>> rows = new ArrayList<>();
                try (org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVParser.parse(
                        file, java.nio.charset.StandardCharsets.UTF_8,
                        org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                    int count = 0;
                    for (org.apache.commons.csv.CSVRecord record : parser) {
                        if (count >= 100)
                            break;
                        Map<String, String> row = new HashMap<>();
                        for (String col : request.getColumns()) {
                            row.put(col, record.get(col));
                        }
                        rows.add(row);
                        count++;
                    }
                    return ResponseEntity.ok(rows);
                }
            } else {
                return ResponseEntity.badRequest().body("Invalid source");
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Preview failed: " + e.getMessage());
        }
    }

    // Helper method to convert List<Map<String, String>> to List<JoinCondition>
    // Updated helper method in IngestionController.java
    private List<JoinCondition> convertToJoinConditions(List<Map<String, String>> joinConditionsMap) {
        if (joinConditionsMap == null) {
            return Collections.emptyList();
        }
        List<JoinCondition> joinConditions = new ArrayList<>();
        for (Map<String, String> map : joinConditionsMap) {
            String joinType = map.get("joinType");
            String mainTable = map.get("mainTable");
            String mainColumn = map.get("mainColumn");
            String joinTable = map.get("joinTable");
            String joinColumn = map.get("joinColumn");

            if (joinType != null && mainTable != null && mainColumn != null
                    && joinTable != null && joinColumn != null) {
                joinConditions.add(new JoinCondition(
                        joinType,
                        mainTable,
                        mainColumn,
                        joinTable,
                        joinColumn));
            }
        }
        return joinConditions;
    }
    
}
