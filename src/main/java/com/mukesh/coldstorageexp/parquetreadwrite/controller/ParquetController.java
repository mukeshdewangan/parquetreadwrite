package com.mukesh.coldstorageexp.parquetreadwrite.controller;

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

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("UP");
    }
}
