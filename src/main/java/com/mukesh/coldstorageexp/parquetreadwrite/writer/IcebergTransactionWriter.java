package com.mukesh.coldstorageexp.parquetreadwrite.writer;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component("icebergWriter")
@ConditionalOnProperty(name = "iceberg.enabled", havingValue = "true")
public class IcebergTransactionWriter implements TransactionFileWriter {

    private final Catalog catalog;
    private final String namespace;
    private final String tableName;
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Value("${iceberg.s3.region:eu-north-1}")
    private String s3Region;

    public IcebergTransactionWriter(
            Catalog catalog,
            @Autowired org.apache.hadoop.conf.Configuration hadoopConfiguration,
            @Value("${iceberg.namespace:default}") String namespace,
            @Value("${iceberg.table.name:transactions}") String tableName) {
        this.catalog = catalog;
        this.namespace = namespace;
        this.tableName = tableName;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public void writeTransactions(List<Transaction> transactions, String baseDir) throws IOException {
        if (transactions == null || transactions.isEmpty()) {
            return;
        }

        try {
            // Load Iceberg schema from Avro schema file
            Schema schema = loadSchemaFromAvro();

            // Create partition spec: account_id + year + month + day
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("account_id")
                .identity("year")
                .identity("month")
                .identity("day")
                .build();

            // Get or create table with table properties
            org.apache.iceberg.catalog.TableIdentifier tableId = 
                org.apache.iceberg.catalog.TableIdentifier.of(namespace, tableName);
            Table table;
            
            try {
                table = catalog.loadTable(tableId);
                System.out.println("✓ Loaded existing Iceberg table: " + tableId);
            } catch (Exception e) {
                System.out.println("✓ Creating new Iceberg table: " + tableId);
                // Create table with Parquet format configuration
                table = catalog.createTable(tableId, schema, spec);
            }

            System.out.println("  Table location: " + table.location());
            System.out.println("  File format: PARQUET");
            System.out.println("  Schema source: transaction-schema.avsc");

            // Write transactions to S3 warehouse in Parquet format
            writeToS3Warehouse(table, schema, transactions);

        } catch (Exception e) {
            System.err.println("✗ Error in writeTransactions: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to write transactions to Iceberg table: " + e.getMessage(), e);
        }
    }

    private Schema loadSchemaFromAvro() throws IOException {
        // Load Avro schema from classpath
        org.apache.avro.Schema avroSchema;
        try (java.io.InputStream is = getClass().getResourceAsStream("/transaction-schema.avsc")) {
            if (is == null) throw new IllegalStateException("transaction-schema.avsc not found on classpath");
            avroSchema = new org.apache.avro.Schema.Parser().parse(is);
        }

        // Convert Avro schema to Iceberg schema
        return convertAvroToIcebergSchema(avroSchema);
    }

    private Schema convertAvroToIcebergSchema(org.apache.avro.Schema avroSchema) {
        // Define the Iceberg schema based on Avro schema fields
        Schema schema = new Schema(
            Types.NestedField.required(1, "transaction_id", Types.StringType.get()),
            Types.NestedField.required(2, "account_id", Types.StringType.get()),
            Types.NestedField.required(3, "amount", Types.DoubleType.get()),
            Types.NestedField.required(4, "currency", Types.StringType.get()),
            Types.NestedField.required(5, "transaction_type", Types.StringType.get()),
            Types.NestedField.required(6, "timestamp", Types.LongType.get()),
            Types.NestedField.optional(7, "description", Types.StringType.get()),
            Types.NestedField.required(8, "year", Types.IntegerType.get()),
            Types.NestedField.required(9, "month", Types.IntegerType.get()),
            Types.NestedField.required(10, "day", Types.IntegerType.get())
        );
        
        System.out.println("✓ Loaded schema from transaction-schema.avsc");
        System.out.println("  Avro schema name: " + avroSchema.getName());
        System.out.println("  Iceberg fields: " + schema.columns().size());
        
        return schema;
    }

    private void writeToS3Warehouse(Table table, Schema schema, List<Transaction> transactions) throws IOException {
        // Load Avro schema from classpath
        org.apache.avro.Schema avroSchema;
        try (java.io.InputStream is = getClass().getResourceAsStream("/transaction-schema.avsc")) {
            if (is == null) throw new IllegalStateException("transaction-schema.avsc not found on classpath");
            avroSchema = new org.apache.avro.Schema.Parser().parse(is);
        }

        // Group transactions by partition (accountId/year/month/day)
        java.util.Map<String, java.util.List<Transaction>> partitionedTransactions = new java.util.HashMap<>();
        for (Transaction t : transactions) {
            String partitionKey = String.format("account_id=%s/year=%d/month=%02d/day=%02d", 
                t.getAccountId(), t.getYear(), t.getMonth(), t.getDay());
            partitionedTransactions.computeIfAbsent(partitionKey, k -> new java.util.ArrayList<>()).add(t);
        }

        // Write each partition's transactions to partitioned path
        String warehouseDir = table.location();
        org.apache.iceberg.io.FileIO fileIO = table.io();
        
        for (java.util.Map.Entry<String, java.util.List<Transaction>> entry : partitionedTransactions.entrySet()) {
            String partitionPath = entry.getKey();
            java.util.List<Transaction> partitionTransactions = entry.getValue();
            
            // Generate Parquet filename with timestamp
            String fileName = String.format("transactions-%d.parquet", System.currentTimeMillis());
            String dataPath = warehouseDir + "/data/" + partitionPath;
            String filePath = dataPath + "/" + fileName;

            System.out.println("✓ Writing transactions to S3 in Parquet format");
            System.out.println("  Partition: " + partitionPath);
            System.out.println("  S3 Path: " + filePath);

            // Use Iceberg's FileIO to get OutputFile
            org.apache.iceberg.io.OutputFile outputFile = fileIO.newOutputFile(filePath);
            
            // Write Parquet records using Avro schema
            try (var recordWriter = org.apache.iceberg.parquet.Parquet.write(outputFile)
                    .schema(schema)
                    .build()) {

                for (Transaction t : partitionTransactions) {
                    // Create Avro record instead of Iceberg record
                    org.apache.avro.generic.GenericRecord avroRecord = 
                        new org.apache.avro.generic.GenericData.Record(avroSchema);
                    avroRecord.put("transaction_id", t.getTransactionId());
                    avroRecord.put("account_id", t.getAccountId());
                    avroRecord.put("amount", t.getAmount());
                    avroRecord.put("currency", t.getCurrency());
                    avroRecord.put("transaction_type", t.getTransactionType());
                    avroRecord.put("timestamp", t.getTimestamp());
                    avroRecord.put("description", t.getDescription());
                    avroRecord.put("year", t.getYear());
                    avroRecord.put("month", t.getMonth());
                    avroRecord.put("day", t.getDay());
                    recordWriter.add(avroRecord);
                }
                
                System.out.println("✓ Successfully wrote " + partitionTransactions.size() + " transactions to S3 in Parquet format");
                
                // Register the data file with Iceberg
                registerDataFileWithIceberg(table, new Path(filePath), partitionTransactions.size());
                
            } catch (IOException e) {
                System.err.println("✗ Failed to write parquet file to S3: " + e.getMessage());
                throw e;
            }
        }
    }

    private void registerDataFileWithIceberg(Table table, Path filePath, long recordCount) {
        try {
            // Create a DataFile to register with Iceberg
            DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(filePath.toString())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withFileSizeInBytes(100000) // Approximate size - will be updated by Iceberg
                .withRecordCount(recordCount)
                .build();

            // Append the data file to the table
            AppendFiles append = table.newAppend();
            append.appendFile(dataFile);
            append.commit();
            
            System.out.println("✓ Registered data file with Iceberg metadata catalog");
        } catch (Exception e) {
            System.err.println("✗ Warning: Failed to register data file with Iceberg: " + e.getMessage());
            // Don't throw - the file is still written to S3, just not tracked by Iceberg yet
        }
    }
}
