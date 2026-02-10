package com.mukesh.coldstorageexp.parquetreadwrite.model;

import java.util.HashMap;
import java.util.Map;

public class MetadataStats {
    
    private String accountId;
    private Integer year;
    private Integer month;
    private Integer day;
    private long fileCount;
    private long totalSizeBytes;
    private long totalRecordCount;
    private Map<String, ColumnStats> columnStats;
    private long minTimestamp;
    private long maxTimestamp;
    private double minAmount;
    private double maxAmount;
    
    public MetadataStats() {
        this.columnStats = new HashMap<>();
    }
    
    public MetadataStats(String accountId, Integer year, Integer month, Integer day) {
        this();
        this.accountId = accountId;
        this.year = year;
        this.month = month;
        this.day = day;
    }
    
    // Getters and Setters
    public String getAccountId() {
        return accountId;
    }
    
    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }
    
    public Integer getYear() {
        return year;
    }
    
    public void setYear(Integer year) {
        this.year = year;
    }
    
    public Integer getMonth() {
        return month;
    }
    
    public void setMonth(Integer month) {
        this.month = month;
    }
    
    public Integer getDay() {
        return day;
    }
    
    public void setDay(Integer day) {
        this.day = day;
    }
    
    public long getFileCount() {
        return fileCount;
    }
    
    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }
    
    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }
    
    public void setTotalSizeBytes(long totalSizeBytes) {
        this.totalSizeBytes = totalSizeBytes;
    }
    
    public long getTotalRecordCount() {
        return totalRecordCount;
    }
    
    public void setTotalRecordCount(long totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
    }
    
    public Map<String, ColumnStats> getColumnStats() {
        return columnStats;
    }
    
    public void setColumnStats(Map<String, ColumnStats> columnStats) {
        this.columnStats = columnStats;
    }
    
    public long getMinTimestamp() {
        return minTimestamp;
    }
    
    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }
    
    public long getMaxTimestamp() {
        return maxTimestamp;
    }
    
    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }
    
    public double getMinAmount() {
        return minAmount;
    }
    
    public void setMinAmount(double minAmount) {
        this.minAmount = minAmount;
    }
    
    public double getMaxAmount() {
        return maxAmount;
    }
    
    public void setMaxAmount(double maxAmount) {
        this.maxAmount = maxAmount;
    }
    
    @Override
    public String toString() {
        return "MetadataStats{" +
                "accountId='" + accountId + '\'' +
                ", year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", fileCount=" + fileCount +
                ", totalSizeBytes=" + totalSizeBytes +
                ", totalRecordCount=" + totalRecordCount +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", minAmount=" + minAmount +
                ", maxAmount=" + maxAmount +
                '}';
    }
    
    // Inner class for column statistics
    public static class ColumnStats {
        private long min;
        private long max;
        
        public ColumnStats(long min, long max) {
            this.min = min;
            this.max = max;
        }
        
        public long getMin() {
            return min;
        }
        
        public void setMin(long min) {
            this.min = min;
        }
        
        public long getMax() {
            return max;
        }
        
        public void setMax(long max) {
            this.max = max;
        }
    }
}
