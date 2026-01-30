package com.mukesh.coldstorageexp.parquetreadwrite.demo;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import com.mukesh.coldstorageexp.parquetreadwrite.service.ParquetService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "parquet.demo.enabled", havingValue = "true")
public class ParquetDemoRunner implements CommandLineRunner {

    private final ParquetService parquetService;

    public ParquetDemoRunner(ParquetService parquetService) {
        this.parquetService = parquetService;
    }

    @Override
    public void run(String... args) throws Exception {
        String baseDir = "."; // write into project root by default

        Transaction t1 = new Transaction("tx-1", "acc-1", 100.0, "USD", "CREDIT", Instant.now().toEpochMilli(), "Salary");
        Transaction t2 = new Transaction("tx-2", "acc-1", 25.5, "USD", "DEBIT", Instant.now().toEpochMilli(), "Coffee");

        parquetService.writeTransactions(Arrays.asList(t1, t2), baseDir);

        long start = Instant.now().minusSeconds(3600).toEpochMilli();
        long end = Instant.now().plusSeconds(3600).toEpochMilli();

        var read = parquetService.readTransactions("acc-1", start, end, baseDir);
        System.out.println("Read transactions: " + read);
    }
}
