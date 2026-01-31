package com.mukesh.coldstorageexp.parquetreadwrite.writer;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Component("localWriter")
@ConditionalOnProperty(name = "parquet.writer", havingValue = "local", matchIfMissing = true)
public class LocalTransactionFileWriter implements TransactionFileWriter {

    private final Schema schema;

    public LocalTransactionFileWriter() {
        try (InputStream is = getClass().getResourceAsStream("/transaction.avsc")) {
            if (is == null) {
                throw new IllegalStateException("transaction.avsc not found on classpath");
            }
            this.schema = new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema", e);
        }
    }

    @Override
    public void writeTransactions(List<Transaction> transactions, String baseDir) throws IOException {
        if (transactions == null || transactions.isEmpty()) return;

        Transaction first = transactions.get(0);
        String partitionDir = TransactionPartitioner.partitionDir(baseDir, first.getAccountId(), first.getTimestamp());

        Files.createDirectories(Paths.get(partitionDir));
        String fileName = TransactionPartitioner.timeSortableFileName(transactions);
        String filePath = Paths.get(partitionDir, fileName).toString();

        Configuration conf = new Configuration();
        Path hadoopPath = new Path(filePath);

        try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(1048576)
                .build()) {

            for (Transaction t : transactions) {
                GenericRecord rec = new GenericData.Record(schema);
                rec.put("transaction_id", t.getTransactionId());
                rec.put("account_id", t.getAccountId());
                rec.put("amount", t.getAmount());
                rec.put("currency", t.getCurrency());
                rec.put("transaction_type", t.getTransactionType());
                rec.put("timestamp", t.getTimestamp());
                rec.put("description", t.getDescription() == null ? "" : t.getDescription());
                writer.write(rec);
            }
        }
    }
}
