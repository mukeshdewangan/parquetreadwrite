package com.mukesh.coldstorageexp.parquetreadwrite.service;

import com.mukesh.coldstorageexp.parquetreadwrite.model.MetadataStats;
import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import com.mukesh.coldstorageexp.parquetreadwrite.reader.DuckDBTransactionReader;
import com.mukesh.coldstorageexp.parquetreadwrite.reader.IcebergMetadataReader;
import com.mukesh.coldstorageexp.parquetreadwrite.reader.IcebergTransactionReader;
import com.mukesh.coldstorageexp.parquetreadwrite.writer.TransactionFileWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class ParquetService {

    private final TransactionFileWriter fileWriter;
    
    @Autowired(required = false)
    private IcebergTransactionReader icebergTransactionReader;
    
    @Autowired(required = false)
    private DuckDBTransactionReader duckDBTransactionReader;
    
    @Autowired(required = false)
    private IcebergMetadataReader icebergMetadataReader;

    public ParquetService(@org.springframework.beans.factory.annotation.Qualifier("icebergWriter") TransactionFileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public void writeTransactions(List<Transaction> transactions, String baseDir) throws IOException {
        fileWriter.writeTransactions(transactions, baseDir);
    }

    /**
     * Read transactions using Iceberg-native metadata-driven scanning
     * Falls back to filesystem scanning if Iceberg is not enabled
     */
    public List<Transaction> readTransactions(String accountId, long startTsInclusive, long endTsExclusive, String baseDir) throws IOException {
        if (icebergTransactionReader != null) {
            System.out.println("✓ Using Iceberg-native reader for metadata-driven query");
            return icebergTransactionReader.readTransactionsByAccountAndTimeRange(accountId, startTsInclusive, endTsExclusive);
        }
        
        // Fallback to filesystem-based reading if Iceberg is not available
        System.out.println("⚠ Iceberg reader not available, falling back to filesystem scan");
        return readTransactionsFilesystemBased(accountId, startTsInclusive, endTsExclusive, baseDir);
    }
    
    /**
     * Read transactions for a specific account, year, and month using Iceberg
     */
    public List<Transaction> readTransactionsByAccountYearMonth(String accountId, Integer year, Integer month) throws IOException {
        if (icebergTransactionReader != null) {
            System.out.println("✓ Using Iceberg-native reader for account/year/month query");
            return icebergTransactionReader.readTransactionsByAccountYearMonth(accountId, year, month);
        }
        
        System.out.println("⚠ Iceberg reader not available");
        return new ArrayList<>();
    }
    
    /**
     * Read all transactions for an account using Iceberg
     */
    public List<Transaction> readTransactionsByAccount(String accountId) throws IOException {
        if (icebergTransactionReader != null) {
            System.out.println("✓ Using Iceberg-native reader for account query");
            return icebergTransactionReader.readTransactionsByAccount(accountId);
        }
        
        System.out.println("⚠ Iceberg reader not available");
        return new ArrayList<>();
    }
    
    /**
     * Get metadata statistics for a partition (account + year + month + optional day)
     */
    public MetadataStats getPartitionMetadata(String accountId, Integer year, Integer month, Integer day) {
        if (icebergMetadataReader != null) {
            return icebergMetadataReader.getPartitionStats(accountId, year, month, day);
        }
        
        System.out.println("⚠ Iceberg metadata reader not available");
        return new MetadataStats(accountId, year, month, day);
    }
    
    /**
     * Get account-level metadata statistics
     */
    public MetadataStats getAccountMetadata(String accountId) {
        if (icebergMetadataReader != null) {
            return icebergMetadataReader.getAccountStats(accountId);
        }
        
        System.out.println("⚠ Iceberg metadata reader not available");
        return new MetadataStats(accountId, null, null, null);
    }
    
    /**
     * Get year-level metadata statistics
     */
    public MetadataStats getYearMetadata(String accountId, Integer year) {
        if (icebergMetadataReader != null) {
            return icebergMetadataReader.getYearStats(accountId, year);
        }
        
        System.out.println("⚠ Iceberg metadata reader not available");
        return new MetadataStats(accountId, year, null, null);
    }

    // ----------------------------------------------------------------
    // DuckDB-powered reads (Iceberg tables from S3 via DuckDB engine)
    // ----------------------------------------------------------------

    /**
     * Read transactions using DuckDB's iceberg_scan() for a time range
     */
    public List<Transaction> readTransactionsDuckDB(String accountId, long startTsInclusive, long endTsExclusive) throws IOException {
        if (duckDBTransactionReader != null) {
            System.out.println("✓ Using DuckDB reader for time-range query");
            return duckDBTransactionReader.readTransactionsByAccountAndTimeRange(accountId, startTsInclusive, endTsExclusive);
        }
        System.out.println("⚠ DuckDB reader not available (duckdb.enabled=false?)");
        return new ArrayList<>();
    }

    /**
     * Read transactions using DuckDB for account/year/month
     */
    public List<Transaction> readTransactionsByAccountYearMonthDuckDB(String accountId, Integer year, Integer month) throws IOException {
        if (duckDBTransactionReader != null) {
            System.out.println("✓ Using DuckDB reader for account/year/month query");
            return duckDBTransactionReader.readTransactionsByAccountYearMonth(accountId, year, month);
        }
        System.out.println("⚠ DuckDB reader not available");
        return new ArrayList<>();
    }

    /**
     * Read all transactions for an account using DuckDB
     */
    public List<Transaction> readTransactionsByAccountDuckDB(String accountId) throws IOException {
        if (duckDBTransactionReader != null) {
            System.out.println("✓ Using DuckDB reader for account query");
            return duckDBTransactionReader.readTransactionsByAccount(accountId);
        }
        System.out.println("⚠ DuckDB reader not available");
        return new ArrayList<>();
    }

    /**
     * Run an ad-hoc DuckDB SQL WHERE clause against the Iceberg table
     */
    public List<Transaction> queryTransactionsDuckDB(String whereClause) throws IOException {
        if (duckDBTransactionReader != null) {
            System.out.println("✓ Using DuckDB reader for ad-hoc query");
            return duckDBTransactionReader.queryTransactions(whereClause);
        }
        System.out.println("⚠ DuckDB reader not available");
        return new ArrayList<>();
    }

    /**
     * Legacy filesystem-based transaction reader (fallback)
     */
    private List<Transaction> readTransactionsFilesystemBased(String accountId, long startTsInclusive, long endTsExclusive, String baseDir) throws IOException {
        List<Transaction> results = new ArrayList<>();
        
        String accountDir = java.nio.file.Paths.get(baseDir, "parquet-data", "transactions", "account_id=" + accountId).toString();
        if (!java.nio.file.Files.exists(java.nio.file.Paths.get(accountDir))) return results;

        java.nio.file.Files.walk(java.nio.file.Paths.get(accountDir))
                .filter(p -> p.toString().endsWith(".parquet"))
                .forEach(p -> {
                    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(p.toAbsolutePath().toString());
                    try (org.apache.parquet.avro.AvroParquetReader<org.apache.avro.generic.GenericRecord> reader = 
                         new org.apache.parquet.avro.AvroParquetReader<>(hPath)) {
                        org.apache.avro.generic.GenericRecord rec;
                        while ((rec = reader.read()) != null) {
                            long ts = (Long) rec.get("timestamp");
                            String acc = rec.get("account_id").toString();
                            if (!accountId.equals(acc)) continue;
                            if (ts < startTsInclusive || ts >= endTsExclusive) continue;

                            Transaction t = new Transaction();
                            t.setTransactionId(rec.get("transaction_id").toString());
                            t.setAccountId(acc);
                            t.setAmount((Double) rec.get("amount"));
                            t.setCurrency(rec.get("currency").toString());
                            t.setTransactionType(rec.get("transaction_type").toString());
                            t.setTimestamp(ts);
                            t.setDescription(rec.get("description").toString());
                            results.add(t);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        return results;
    }
}
