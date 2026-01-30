package com.mukesh.coldstorageexp.parquetreadwrite.writer;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

public final class TransactionPartitioner {

    private TransactionPartitioner() {}

    public static String partitionDir(String baseDir, String accountId, long timestamp) {
        OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        String year = String.format("%04d", odt.getYear());
        String month = String.format("%02d", odt.getMonthValue());
        String day = String.format("%02d", odt.getDayOfMonth());

        return java.nio.file.Paths.get(Objects.requireNonNull(baseDir), "parquet-data", "transactions",
                "account_id=" + accountId,
                "year=" + year,
                "month=" + month,
                "day=" + day).toString();
    }

    public static String timeSortableFileName(List<Transaction> transactions) {
        long minTs = transactions.stream().mapToLong(Transaction::getTimestamp).min().orElse(Instant.now().toEpochMilli());
        return String.format("transactions-%d-%d.parquet", minTs, System.nanoTime());
    }
}
