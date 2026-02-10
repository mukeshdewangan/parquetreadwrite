package com.mukesh.coldstorageexp.parquetreadwrite.reader;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "iceberg.enabled", havingValue = "true", matchIfMissing = true)
public class IcebergTransactionReader {
    
    private final Catalog catalog;
    private final String namespace;
    private final String tableName;
    
    public IcebergTransactionReader(
            Catalog catalog,
            @Value("${iceberg.namespace:default}") String namespace,
            @Value("${iceberg.table.name:transactions}") String tableName) {
        this.catalog = catalog;
        this.namespace = namespace;
        this.tableName = tableName;
    }
    
    /**
     * Read transactions for a specific account within a time range using Iceberg metadata-driven scanning
     * Reads from PostgreSQL catalog and accesses files from S3 warehouse
     */
    public List<Transaction> readTransactionsByAccountAndTimeRange(
            String accountId, 
            long startTsInclusive, 
            long endTsExclusive) throws IOException {
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            Table table = catalog.loadTable(tableId);
            
            // Build filter expression: account_id AND timestamp >= startTs AND timestamp < endTs
            Expression filter = Expressions.and(
                Expressions.equal("account_id", accountId),
                Expressions.greaterThanOrEqual("timestamp", startTsInclusive),
                Expressions.lessThan("timestamp", endTsExclusive)
            );
            
            return scanAndCollateTransactions(table, filter, accountId);
            
        } catch (Exception e) {
            System.err.println("✗ Error reading transactions by time range: " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    /**
     * Read transactions for a specific account, year, and month using Iceberg metadata-driven scanning
     * Queries PostgreSQL catalog and reads Parquet files from S3
     */
    public List<Transaction> readTransactionsByAccountYearMonth(
            String accountId,
            Integer year,
            Integer month) throws IOException {
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            Table table = catalog.loadTable(tableId);
            
            // Build filter: account_id AND year AND month
            Expression filter = Expressions.and(
                Expressions.equal("account_id", accountId),
                Expressions.equal("year", year),
                Expressions.equal("month", month)
            );
            
            return scanAndCollateTransactions(table, filter, accountId);
            
        } catch (Exception e) {
            System.err.println("✗ Error reading transactions by account/year/month: " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    /**
     * Read all transactions for a specific account using Iceberg metadata-driven scanning
     * Leverages partition pruning from Iceberg metadata in PostgreSQL catalog
     */
    public List<Transaction> readTransactionsByAccount(String accountId) throws IOException {
        
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            Table table = catalog.loadTable(tableId);
            
            // Filter only by account_id to get all partitions for that account
            Expression filter = Expressions.equal("account_id", accountId);
            
            return scanAndCollateTransactions(table, filter, accountId);
            
        } catch (Exception e) {
            System.err.println("✗ Error reading all transactions for account: " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    /**
     * Internal method to scan Iceberg table and collate transaction results
     * Handles actual Parquet file reading from S3 or local filesystem
     * Falls back to direct file scanning if catalog is unavailable or returns no results
     */
    private List<Transaction> scanAndCollateTransactions(Table table, Expression filter, String expectedAccountId) throws IOException {
        List<Transaction> results = new ArrayList<>();
        
        System.out.println("✓ Scanning Iceberg table: " + namespace + "." + tableName);
        System.out.println("  Filter: " + filter);
        
        long filesRead = 0;
        long recordsRead = 0;
        boolean catalogFailed = false;
        
        try {
            try (CloseableIterable<FileScanTask> tasks = table.newScan()
                    .filter(filter)
                    .planFiles()) {
                
                for (FileScanTask task : tasks) {
                    // Get the actual file path from the data file
                    String filePath = task.file().path().toString();
                    System.out.println("  Reading file: " + filePath);
                    filesRead++;
                    
                    // Read Parquet file from S3 or local path
                    try (AvroParquetReader<GenericRecord> reader = 
                         new AvroParquetReader<>(new Path(filePath))) {
                        
                        GenericRecord record;
                        while ((record = reader.read()) != null) {
                            try {
                                Transaction tx = mapGenericRecordToTransaction(record, expectedAccountId);
                                if (tx != null) {
                                    results.add(tx);
                                    recordsRead++;
                                }
                            } catch (Exception e) {
                                System.err.println("    Warning: Failed to map record to transaction: " + e.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("✗ Error reading Parquet file: " + filePath + " - " + e.getMessage());
                        throw e;
                    }
                }
            }
            
            // If catalog found no files, try fallback anyway
            if (filesRead == 0) {
                System.out.println("⚠ Catalog returned 0 files, attempting fallback to filesystem scan...");
                List<Transaction> fallbackResults = fallbackFilesystemScan(expectedAccountId, filter);
                if (!fallbackResults.isEmpty()) {
                    return fallbackResults;
                }
            }
            
        } catch (Exception e) {
            System.out.println("⚠ Catalog scan failed: " + e.getMessage());
            System.out.println("  Falling back to direct filesystem scan...");
            catalogFailed = true;
            return fallbackFilesystemScan(expectedAccountId, filter);
        }
        
        System.out.println("✓ Scan complete: " + filesRead + " files, " + recordsRead + " transactions");
        return results;
    }
    
    /**
     * Fallback method to scan local parquet-data directory directly
     * Used when Iceberg catalog is unavailable
     */
    private List<Transaction> fallbackFilesystemScan(String accountId, Expression filter) throws IOException {
        List<Transaction> results = new ArrayList<>();
        
        System.out.println("✓ Using fallback filesystem scan");
        System.out.println("  Account: " + accountId);
        
        java.nio.file.Path baseDir = java.nio.file.Paths.get("parquet-data", "transactions", 
                "account_id=" + accountId);
        
        if (!java.nio.file.Files.exists(baseDir)) {
            System.out.println("  No files found for account: " + accountId);
            return results;
        }
        
        AtomicLong filesRead = new AtomicLong(0);
        AtomicLong recordsRead = new AtomicLong(0);
        
        try {
            java.nio.file.Files.walk(baseDir)
                    .filter(p -> p.toString().endsWith(".parquet"))
                    .forEach(p -> {
                        try {
                            System.out.println("  Reading file: " + p);
                            filesRead.incrementAndGet();
                            
                            org.apache.hadoop.fs.Path hadoopPath = 
                                new org.apache.hadoop.fs.Path(p.toAbsolutePath().toString());
                            
                            try (AvroParquetReader<GenericRecord> reader = 
                                 new AvroParquetReader<>(hadoopPath)) {
                                
                                GenericRecord record;
                                while ((record = reader.read()) != null) {
                                    try {
                                        Transaction tx = mapGenericRecordToTransaction(record, accountId);
                                        if (tx != null) {
                                            results.add(tx);
                                            recordsRead.incrementAndGet();
                                        }
                                    } catch (Exception e) {
                                        System.err.println("    Warning: Failed to map record: " + e.getMessage());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("  Error reading file: " + p + " - " + e.getMessage());
                        }
                    });
        } catch (Exception e) {
            System.err.println("✗ Error in fallback filesystem scan: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("✓ Fallback scan complete: " + filesRead.get() + " files, " + recordsRead.get() + " transactions");
        return results;
    }
    
    /**
     * Maps Avro GenericRecord from Parquet to Transaction domain model
     */
    private Transaction mapGenericRecordToTransaction(GenericRecord record, String expectedAccountId) {
        try {
            String accountId = record.get("account_id").toString();
            
            // Filter check: ensure account matches (should be guaranteed by Iceberg filter, but verify)
            if (!expectedAccountId.equals(accountId)) {
                return null;
            }
            
            Transaction tx = new Transaction();
            tx.setTransactionId(record.get("transaction_id").toString());
            tx.setAccountId(accountId);
            tx.setAmount((Double) record.get("amount"));
            tx.setCurrency(record.get("currency").toString());
            tx.setTransactionType(record.get("transaction_type").toString());
            tx.setTimestamp((Long) record.get("timestamp"));
            tx.setDescription(record.get("description").toString());
            
            return tx;
        } catch (Exception e) {
            System.err.println("Error mapping record: " + e.getMessage());
            return null;
        }
    }
}
