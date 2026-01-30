package com.mukesh.coldstorageexp.parquetreadwrite.writer;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import java.io.IOException;
import java.util.List;

public interface TransactionFileWriter {
    void writeTransactions(List<Transaction> transactions, String baseDir) throws IOException;
}
