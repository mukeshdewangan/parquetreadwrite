package com.mukesh.coldstorageexp.parquetreadwrite.writer;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

@Component
@ConditionalOnProperty(name = "parquet.writer", havingValue = "s3")
public class RemoteTransactionFileWriter implements TransactionFileWriter {

    private final org.apache.avro.Schema schema;
    private final S3Client s3;
    private final String bucket;
    private final String prefix;

    public RemoteTransactionFileWriter(@Value("${parquet.s3.bucket}") String bucket,
                                       @Value("${parquet.s3.prefix:}") String prefix,
                                       @Value("${parquet.s3.region:}") String region) {
        try (InputStream is = getClass().getResourceAsStream("/transaction.avsc")) {
            if (is == null) throw new IllegalStateException("transaction.avsc not found on classpath");
            this.schema = new org.apache.avro.Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.bucket = bucket;
        this.prefix = (prefix == null) ? "" : prefix;
        Region reg = (region == null || region.isBlank()) ? Region.US_EAST_1 : Region.of(region);
        this.s3 = S3Client.builder().region(reg).build();
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

        // Upload to S3
        java.nio.file.Path local = java.nio.file.Paths.get(filePath);
        // Build key using partitioner logic to keep path consistent
        OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(first.getTimestamp()), ZoneId.systemDefault());
        String year = String.format("%04d", odt.getYear());
        String month = String.format("%02d", odt.getMonthValue());
        String day = String.format("%02d", odt.getDayOfMonth());

        String key = String.format("%s/transactions/account_id=%s/year=%s/month=%s/day=%s/%s",
            prefix.replaceAll("^/+|/+$", ""), first.getAccountId(), year, month, day, fileName);
        if (key.startsWith("/")) key = key.substring(1);

        PutObjectRequest por = PutObjectRequest.builder().bucket(bucket).key(key).build();
        s3.putObject(por, RequestBody.fromFile(local.toFile()));

        // Optionally remove local file
        // Files.deleteIfExists(local);
    }
}
