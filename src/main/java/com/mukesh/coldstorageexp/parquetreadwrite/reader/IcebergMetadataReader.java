package com.mukesh.coldstorageexp.parquetreadwrite.reader;

import com.mukesh.coldstorageexp.parquetreadwrite.model.MetadataStats;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "iceberg.enabled", havingValue = "true")
public class IcebergMetadataReader {
    
    private final Catalog catalog;
    private final String namespace;
    private final String tableName;
    private final String warehousePath;
    
    public IcebergMetadataReader(
            Catalog catalog,
            @Value("${iceberg.namespace:default}") String namespace,
            @Value("${iceberg.table.name:transactions}") String tableName,
            @Value("${iceberg.catalog.warehouse}") String warehousePath) {
        this.catalog = catalog;
        this.namespace = namespace;
        this.tableName = tableName;
        this.warehousePath = warehousePath;
        
        System.out.println("✓ Initialized IcebergMetadataReader");
        System.out.println("  Namespace: " + namespace);
        System.out.println("  Table: " + tableName);
        System.out.println("  Warehouse: " + warehousePath);
    }
    
    /**
     * Get metadata statistics for a specific partition (account + year + month + optional day)
     * Reads from PostgreSQL catalog and identifies files in S3 warehouse
     */
    public MetadataStats getPartitionStats(String accountId, Integer year, Integer month, Integer day) {
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            System.out.println("✓ Loading table from PostgreSQL catalog: " + tableId);
            Table table = catalog.loadTable(tableId);
            
            MetadataStats stats = new MetadataStats(accountId, year, month, day);
            
            // Build filter expression: account_id AND year AND month AND (day if provided)
            Expression filter = Expressions.and(
                Expressions.equal("account_id", accountId),
                Expressions.equal("year", year),
                Expressions.equal("month", month)
            );
            
            if (day != null) {
                filter = Expressions.and(filter, Expressions.equal("day", day));
            }
            
            System.out.println("  Filter expression: account_id=" + accountId + ", year=" + year + 
                    ", month=" + month + (day != null ? ", day=" + day : ""));
            
            // Scan table with filter to get files from S3 warehouse
            long fileCount = 0;
            long totalSizeBytes = 0;
            long totalRecordCount = 0;
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            double minAmount = Double.MAX_VALUE;
            double maxAmount = Double.MIN_VALUE;
            
            try (CloseableIterable<FileScanTask> tasks = table.newScan()
                    .filter(filter)
                    .planFiles()) {
                
                for (FileScanTask task : tasks) {
                    DataFile dataFile = task.file();
                    fileCount++;
                    totalSizeBytes += dataFile.fileSizeInBytes();
                    totalRecordCount += dataFile.recordCount();
                    
                    // Log file information
                    String filePath = dataFile.path().toString();
                    System.out.println("    File: " + filePath + 
                            " (size=" + formatBytes(dataFile.fileSizeInBytes()) + 
                            ", records=" + dataFile.recordCount() + ")");
                    
                    // Extract column statistics if available
                    Map<Integer, java.nio.ByteBuffer> lowerBounds = dataFile.lowerBounds();
                    Map<Integer, java.nio.ByteBuffer> upperBounds = dataFile.upperBounds();
                    
                    if (lowerBounds != null && upperBounds != null) {
                        // Get timestamp bounds (field ID 6 for timestamp)
                        try {
                            if (lowerBounds.containsKey(6) && upperBounds.containsKey(6)) {
                                long tsMin = lowerBounds.get(6).getLong(0);
                                long tsMax = upperBounds.get(6).getLong(0);
                                minTimestamp = Math.min(minTimestamp, tsMin);
                                maxTimestamp = Math.max(maxTimestamp, tsMax);
                            }
                        } catch (Exception e) {
                            // Column stats may not be available
                        }
                        
                        // Get amount bounds (field ID 3 for amount)
                        try {
                            if (lowerBounds.containsKey(3) && upperBounds.containsKey(3)) {
                                // Amount is double, encoded as long
                                long amountLower = lowerBounds.get(3).getLong(0);
                                long amountUpper = upperBounds.get(3).getLong(0);
                                minAmount = Math.min(minAmount, Double.longBitsToDouble(amountLower));
                                maxAmount = Math.max(maxAmount, Double.longBitsToDouble(amountUpper));
                            }
                        } catch (Exception e) {
                            // Column stats may not be available
                        }
                    }
                }
            }
            
            stats.setFileCount(fileCount);
            stats.setTotalSizeBytes(totalSizeBytes);
            stats.setTotalRecordCount(totalRecordCount);
            
            if (minTimestamp != Long.MAX_VALUE) {
                stats.setMinTimestamp(minTimestamp);
            }
            if (maxTimestamp != Long.MIN_VALUE) {
                stats.setMaxTimestamp(maxTimestamp);
            }
            if (minAmount != Double.MAX_VALUE) {
                stats.setMinAmount(minAmount);
            }
            if (maxAmount != Double.MIN_VALUE) {
                stats.setMaxAmount(maxAmount);
            }
            
            System.out.println("✓ Metadata Stats for " + accountId + "/" + year + "/" + month + 
                    (day != null ? "/" + day : ""));
            System.out.println("  Files: " + fileCount + ", Total Size: " + formatBytes(totalSizeBytes) + 
                    ", Records: " + totalRecordCount);
            
            return stats;
            
        } catch (Exception e) {
            System.err.println("✗ Error reading metadata from PostgreSQL catalog: " + e.getMessage());
            e.printStackTrace();
            return new MetadataStats(accountId, year, month, day);
        }
    }
    
    /**
     * Get account-level statistics across all time periods
     * Uses PostgreSQL catalog to aggregate metadata from all S3 files
     */
    public MetadataStats getAccountStats(String accountId) {
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            System.out.println("✓ Loading table from PostgreSQL catalog for account stats: " + tableId);
            Table table = catalog.loadTable(tableId);
            
            MetadataStats stats = new MetadataStats(accountId, null, null, null);
            
            Expression filter = Expressions.equal("account_id", accountId);
            
            long fileCount = 0;
            long totalSizeBytes = 0;
            long totalRecordCount = 0;
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            double minAmount = Double.MAX_VALUE;
            double maxAmount = Double.MIN_VALUE;
            
            try (CloseableIterable<FileScanTask> tasks = table.newScan()
                    .filter(filter)
                    .planFiles()) {
                
                for (FileScanTask task : tasks) {
                    DataFile dataFile = task.file();
                    fileCount++;
                    totalSizeBytes += dataFile.fileSizeInBytes();
                    totalRecordCount += dataFile.recordCount();
                    
                    Map<Integer, java.nio.ByteBuffer> lowerBounds = dataFile.lowerBounds();
                    Map<Integer, java.nio.ByteBuffer> upperBounds = dataFile.upperBounds();
                    
                    if (lowerBounds != null && upperBounds != null) {
                        try {
                            if (lowerBounds.containsKey(6) && upperBounds.containsKey(6)) {
                                minTimestamp = Math.min(minTimestamp, lowerBounds.get(6).getLong(0));
                                maxTimestamp = Math.max(maxTimestamp, upperBounds.get(6).getLong(0));
                            }
                        } catch (Exception e) {
                            // Stats unavailable
                        }
                        
                        try {
                            if (lowerBounds.containsKey(3) && upperBounds.containsKey(3)) {
                                long amountLower = lowerBounds.get(3).getLong(0);
                                long amountUpper = upperBounds.get(3).getLong(0);
                                minAmount = Math.min(minAmount, Double.longBitsToDouble(amountLower));
                                maxAmount = Math.max(maxAmount, Double.longBitsToDouble(amountUpper));
                            }
                        } catch (Exception e) {
                            // Stats unavailable
                        }
                    }
                }
            }
            
            stats.setFileCount(fileCount);
            stats.setTotalSizeBytes(totalSizeBytes);
            stats.setTotalRecordCount(totalRecordCount);
            
            if (minTimestamp != Long.MAX_VALUE) {
                stats.setMinTimestamp(minTimestamp);
            }
            if (maxTimestamp != Long.MIN_VALUE) {
                stats.setMaxTimestamp(maxTimestamp);
            }
            if (minAmount != Double.MAX_VALUE) {
                stats.setMinAmount(minAmount);
            }
            if (maxAmount != Double.MIN_VALUE) {
                stats.setMaxAmount(maxAmount);
            }
            
            System.out.println("✓ Account-level Metadata Stats for " + accountId);
            System.out.println("  Files: " + fileCount + ", Total Size: " + formatBytes(totalSizeBytes) + 
                    ", Records: " + totalRecordCount);
            
            return stats;
            
        } catch (Exception e) {
            System.err.println("✗ Error reading account metadata from PostgreSQL catalog: " + e.getMessage());
            e.printStackTrace();
            return new MetadataStats(accountId, null, null, null);
        }
    }
    
    /**
     * Get year-level statistics for a given account
     * Queries PostgreSQL catalog for all files in a specific year
     */
    public MetadataStats getYearStats(String accountId, Integer year) {
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            System.out.println("✓ Loading table from PostgreSQL catalog for year stats: " + tableId);
            Table table = catalog.loadTable(tableId);
            
            MetadataStats stats = new MetadataStats(accountId, year, null, null);
            
            Expression filter = Expressions.and(
                Expressions.equal("account_id", accountId),
                Expressions.equal("year", year)
            );
            
            long fileCount = 0;
            long totalSizeBytes = 0;
            long totalRecordCount = 0;
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            double minAmount = Double.MAX_VALUE;
            double maxAmount = Double.MIN_VALUE;
            
            try (CloseableIterable<FileScanTask> tasks = table.newScan()
                    .filter(filter)
                    .planFiles()) {
                
                for (FileScanTask task : tasks) {
                    DataFile dataFile = task.file();
                    fileCount++;
                    totalSizeBytes += dataFile.fileSizeInBytes();
                    totalRecordCount += dataFile.recordCount();
                    
                    Map<Integer, java.nio.ByteBuffer> lowerBounds = dataFile.lowerBounds();
                    Map<Integer, java.nio.ByteBuffer> upperBounds = dataFile.upperBounds();
                    
                    if (lowerBounds != null && upperBounds != null) {
                        try {
                            if (lowerBounds.containsKey(6) && upperBounds.containsKey(6)) {
                                minTimestamp = Math.min(minTimestamp, lowerBounds.get(6).getLong(0));
                                maxTimestamp = Math.max(maxTimestamp, upperBounds.get(6).getLong(0));
                            }
                        } catch (Exception e) {
                            // Stats unavailable
                        }
                        
                        try {
                            if (lowerBounds.containsKey(3) && upperBounds.containsKey(3)) {
                                long amountLower = lowerBounds.get(3).getLong(0);
                                long amountUpper = upperBounds.get(3).getLong(0);
                                minAmount = Math.min(minAmount, Double.longBitsToDouble(amountLower));
                                maxAmount = Math.max(maxAmount, Double.longBitsToDouble(amountUpper));
                            }
                        } catch (Exception e) {
                            // Stats unavailable
                        }
                    }
                }
            }
            
            stats.setFileCount(fileCount);
            stats.setTotalSizeBytes(totalSizeBytes);
            stats.setTotalRecordCount(totalRecordCount);
            
            if (minTimestamp != Long.MAX_VALUE) {
                stats.setMinTimestamp(minTimestamp);
            }
            if (maxTimestamp != Long.MIN_VALUE) {
                stats.setMaxTimestamp(maxTimestamp);
            }
            if (minAmount != Double.MAX_VALUE) {
                stats.setMinAmount(minAmount);
            }
            if (maxAmount != Double.MIN_VALUE) {
                stats.setMaxAmount(maxAmount);
            }
            
            System.out.println("✓ Year-level Metadata Stats for " + accountId + "/" + year);
            System.out.println("  Files: " + fileCount + ", Total Size: " + formatBytes(totalSizeBytes) + 
                    ", Records: " + totalRecordCount);
            
            return stats;
            
        } catch (Exception e) {
            System.err.println("✗ Error reading year metadata from PostgreSQL catalog: " + e.getMessage());
            e.printStackTrace();
            return new MetadataStats(accountId, year, null, null);
        }
    }
    
    private String formatBytes(long bytes) {
        if (bytes <= 0) return "0 B";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(bytes) / Math.log10(1024));
        return String.format("%.2f %s", bytes / Math.pow(1024, digitGroups), units[digitGroups]);
    }
}

