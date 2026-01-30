package com.mukesh.coldstorageexp.parquetreadwrite.service;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import com.mukesh.coldstorageexp.parquetreadwrite.writer.TransactionFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class ParquetService {

    private final TransactionFileWriter fileWriter;

    public ParquetService(TransactionFileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public void writeTransactions(List<Transaction> transactions, String baseDir) throws IOException {
        fileWriter.writeTransactions(transactions, baseDir);
    }

    public List<Transaction> readTransactions(String accountId, long startTsInclusive, long endTsExclusive, String baseDir) throws IOException {
        List<Transaction> results = new ArrayList<>();

        String accountDir = Paths.get(baseDir, "parquet-data", "transactions", "account_id=" + accountId).toString();
        if (!Files.exists(Paths.get(accountDir))) return results;

        Files.walk(Paths.get(accountDir))
                .filter(p -> p.toString().endsWith(".parquet"))
                .forEach(p -> {
                    Path hPath = new Path(p.toAbsolutePath().toString());
                    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(hPath)) {
                        GenericRecord rec;
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
