package com.ingestion.model;

import java.util.List;
import java.util.Map;

public class IngestionRequest {
    private String source;
    private String tableName;
    private List<String> columns;
    private String filePath;
    private String jwtToken;
    private List<Map<String, String>> joinConditions;

    // Getters and setters
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public String getJwtToken() { return jwtToken; }
    public void setJwtToken(String jwtToken) { this.jwtToken = jwtToken; }
    public List<Map<String, String>> getJoinConditions() { return joinConditions; }
    public void setJoinConditions(List<Map<String, String>> joinConditions) { this.joinConditions = joinConditions; }
}