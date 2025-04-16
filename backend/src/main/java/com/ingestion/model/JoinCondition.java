package com.ingestion.model;

public class JoinCondition {
    private String joinType;
    private String mainTable;
    private String mainColumn;
    private String joinTable;
    private String joinColumn;

    // Updated constructor and getters/setters
    public JoinCondition(String joinType, String mainTable, String mainColumn, 
                        String joinTable, String joinColumn) {
        this.joinType = joinType;
        this.mainTable = mainTable;
        this.mainColumn = mainColumn;
        this.joinTable = joinTable;
        this.joinColumn = joinColumn;
    }

    // Getters and setters
    public String getjoinType() { return joinType; }
    public void setJoinType(String joinType) { this.joinType = joinType; }
    public String getMainTable() { return mainTable; }
    public void setMainTable(String mainTable) { this.mainTable = mainTable; }

    public String getmaincolumn() { return mainColumn; }
    public void setmaincolumn(String mainColumn) { this.mainColumn = mainColumn; }
    public String getjoinTable() { return joinTable; }
    public void setjoinTable(String joinTable) { this.joinTable = joinTable; }
    public String getjoincolumn() { return joinColumn; }
    public void setJoincolumn(String joinColumn) { this.joinColumn = joinColumn; }
}