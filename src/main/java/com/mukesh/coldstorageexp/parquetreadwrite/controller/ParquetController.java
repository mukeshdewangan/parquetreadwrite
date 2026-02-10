package com.mukesh.coldstorageexp.parquetreadwrite.controller;

import com.mukesh.coldstorageexp.parquetreadwrite.model.MetadataStats;
import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import com.mukesh.coldstorageexp.parquetreadwrite.service.ParquetService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
public class ParquetController {

    private final ParquetService parquetService;
    private final String baseDir;

    public ParquetController(ParquetService parquetService, @Value("${parquet.base-dir:.}") String baseDir) {
        this.parquetService = parquetService;
        this.baseDir = baseDir;
    }

    @PostMapping("/transaction")
    public ResponseEntity<String> addTransaction(@RequestBody Transaction tx) {
        try {
            parquetService.writeTransactions(List.of(tx), baseDir);
            return ResponseEntity.ok("written");
        } catch (IOException e) {
            return ResponseEntity.status(500).body("error: " + e.getMessage());
        }
    }

    @PostMapping("/transactions")
    public ResponseEntity<String> addTransactions(@RequestBody List<Transaction> txs) {
        try {
            parquetService.writeTransactions(txs, baseDir);
            return ResponseEntity.ok("written");
        } catch (IOException e) {
            return ResponseEntity.status(500).body("error: " + e.getMessage());
        }
    }

    @GetMapping("/transactions")
    public ResponseEntity<List<Transaction>> readTransactions(
            @RequestParam String accountId,
            @RequestParam long start,
            @RequestParam long end) {
        try {
            List<Transaction> list = parquetService.readTransactions(accountId, start, end, baseDir);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Read transactions for a specific account, year, and month using Iceberg metadata-driven queries
     */
    @GetMapping("/transactions/account/{accountId}/year/{year}/month/{month}")
    public ResponseEntity<List<Transaction>> readTransactionsByAccountYearMonth(
            @PathVariable String accountId,
            @PathVariable Integer year,
            @PathVariable Integer month) {
        try {
            List<Transaction> list = parquetService.readTransactionsByAccountYearMonth(accountId, year, month);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Read all transactions for a specific account using Iceberg metadata-driven queries
     */
    @GetMapping("/transactions/account/{accountId}")
    public ResponseEntity<List<Transaction>> readTransactionsByAccount(
            @PathVariable String accountId) {
        try {
            List<Transaction> list = parquetService.readTransactionsByAccount(accountId);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Get metadata statistics for a specific partition (account + year + month + optional day)
     * Uses Iceberg catalog metadata without reading data files
     */
    @GetMapping("/metadata/account/{accountId}/year/{year}/month/{month}")
    public ResponseEntity<MetadataStats> getPartitionMetadata(
            @PathVariable String accountId,
            @PathVariable Integer year,
            @PathVariable Integer month,
            @RequestParam(required = false) Integer day) {
        
        MetadataStats stats = parquetService.getPartitionMetadata(accountId, year, month, day);
        return ResponseEntity.ok(stats);
    }

    /**
     * Get account-level metadata statistics
     * Aggregates statistics across all time periods for an account
     */
    @GetMapping("/metadata/account/{accountId}")
    public ResponseEntity<MetadataStats> getAccountMetadata(
            @PathVariable String accountId) {
        
        MetadataStats stats = parquetService.getAccountMetadata(accountId);
        return ResponseEntity.ok(stats);
    }

    /**
     * Get year-level metadata statistics for an account
     * Aggregates statistics across all months in a specific year
     */
    @GetMapping("/metadata/account/{accountId}/year/{year}")
    public ResponseEntity<MetadataStats> getYearMetadata(
            @PathVariable String accountId,
            @PathVariable Integer year) {
        
        MetadataStats stats = parquetService.getYearMetadata(accountId, year);
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("UP");
    }

    // ================================================================
    //  DuckDB-powered endpoints — read Iceberg tables from S3 via DuckDB
    // ================================================================

    /**
     * Read transactions for an account within a time range using DuckDB
     */
    @GetMapping("/duckdb/transactions")
    public ResponseEntity<List<Transaction>> readTransactionsDuckDB(
            @RequestParam String accountId,
            @RequestParam long start,
            @RequestParam long end) {
        try {
            List<Transaction> list = parquetService.readTransactionsDuckDB(accountId, start, end);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Read transactions for account/year/month using DuckDB
     */
    @GetMapping("/duckdb/transactions/account/{accountId}/year/{year}/month/{month}")
    public ResponseEntity<List<Transaction>> readTransactionsByAccountYearMonthDuckDB(
            @PathVariable String accountId,
            @PathVariable Integer year,
            @PathVariable Integer month) {
        try {
            List<Transaction> list = parquetService.readTransactionsByAccountYearMonthDuckDB(accountId, year, month);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Read all transactions for an account using DuckDB
     */
    @GetMapping("/duckdb/transactions/account/{accountId}")
    public ResponseEntity<List<Transaction>> readTransactionsByAccountDuckDB(
            @PathVariable String accountId) {
        try {
            List<Transaction> list = parquetService.readTransactionsByAccountDuckDB(accountId);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Ad-hoc DuckDB SQL query against the Iceberg table.
     * Pass a WHERE clause as request param, e.g. ?where=amount > 100 AND currency = 'USD'
     */
    @GetMapping("/duckdb/transactions/query")
    public ResponseEntity<List<Transaction>> queryTransactionsDuckDB(
            @RequestParam(required = false) String where) {
        try {
            List<Transaction> list = parquetService.queryTransactionsDuckDB(where);
            return ResponseEntity.ok(list);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }
}
