package com.mukesh.coldstorageexp.parquetreadwrite.reader;

import com.mukesh.coldstorageexp.parquetreadwrite.model.Transaction;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Iceberg tables from S3 using DuckDB's in-process SQL engine.
 * <p>
 * DuckDB loads the {@code iceberg} and {@code httpfs} extensions to:
 * <ol>
 *   <li>Authenticate with S3 via httpfs</li>
 *   <li>Query Iceberg table metadata + Parquet data files directly via {@code iceberg_scan()}</li>
 * </ol>
 * This avoids needing a running Iceberg catalog (PostgreSQL) at read time —
 * DuckDB reads the Iceberg metadata JSON/Avro files from S3 directly.
 */
@Component
@ConditionalOnProperty(name = "duckdb.enabled", havingValue = "true")
public class DuckDBTransactionReader {

    @Value("${duckdb.s3.region:eu-north-1}")
    private String s3Region;

    @Value("${duckdb.s3.access-key:}")
    private String s3AccessKey;

    @Value("${duckdb.s3.secret-key:}")
    private String s3SecretKey;

    @Value("${duckdb.iceberg.warehouse}")
    private String warehousePath;

    @Value("${iceberg.namespace:default}")
    private String namespace;

    @Value("${iceberg.table.name:transactions}")
    private String tableName;

    private Connection duckDBConnection;

    @PostConstruct
    public void init() throws SQLException {
        duckDBConnection = DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = duckDBConnection.createStatement()) {
            // Install and load required extensions
            stmt.execute("INSTALL iceberg");
            stmt.execute("LOAD iceberg");
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");

            // Configure S3 credentials
            stmt.execute("SET s3_region = '" + s3Region + "'");

            if (s3AccessKey != null && !s3AccessKey.isBlank()) {
                stmt.execute("SET s3_access_key_id = '" + s3AccessKey + "'");
                stmt.execute("SET s3_secret_access_key = '" + s3SecretKey + "'");
            } else {
                // Use credential_chain provider (IAM role, env vars, ~/.aws/credentials, etc.)
                stmt.execute("SET s3_url_style = 'path'");
                stmt.execute("CREATE SECRET s3_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN)");
            }

            System.out.println("✓ DuckDB initialized with Iceberg + httpfs extensions");
            System.out.println("  S3 Region : " + s3Region);
            System.out.println("  Warehouse : " + warehousePath);
        }
    }

    @PreDestroy
    public void destroy() throws SQLException {
        if (duckDBConnection != null && !duckDBConnection.isClosed()) {
            duckDBConnection.close();
            System.out.println("✓ DuckDB connection closed");
        }
    }

    // ------------------------------------------------------------------
    // Public read methods — mirror IcebergTransactionReader's API
    // ------------------------------------------------------------------

    /**
     * Read transactions for a specific account within a timestamp range.
     */
    public List<Transaction> readTransactionsByAccountAndTimeRange(
            String accountId,
            long startTsInclusive,
            long endTsExclusive) throws IOException {

        String sql = "SELECT * FROM iceberg_scan('" + icebergTablePath() + "') "
                + "WHERE account_id = ? "
                + "  AND timestamp >= ? "
                + "  AND timestamp <  ?";

        try (PreparedStatement ps = duckDBConnection.prepareStatement(sql)) {
            ps.setString(1, accountId);
            ps.setLong(2, startTsInclusive);
            ps.setLong(3, endTsExclusive);
            return executeAndMap(ps);
        } catch (SQLException e) {
            throw new IOException("DuckDB query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Read transactions for a specific account, year, and month.
     */
    public List<Transaction> readTransactionsByAccountYearMonth(
            String accountId,
            Integer year,
            Integer month) throws IOException {

        String sql = "SELECT * FROM iceberg_scan('" + icebergTablePath() + "') "
                + "WHERE account_id = ? "
                + "  AND year  = ? "
                + "  AND month = ?";

        try (PreparedStatement ps = duckDBConnection.prepareStatement(sql)) {
            ps.setString(1, accountId);
            ps.setInt(2, year);
            ps.setInt(3, month);
            return executeAndMap(ps);
        } catch (SQLException e) {
            throw new IOException("DuckDB query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Read all transactions for a specific account.
     */
    public List<Transaction> readTransactionsByAccount(String accountId) throws IOException {

        String sql = "SELECT * FROM iceberg_scan('" + icebergTablePath() + "') "
                + "WHERE account_id = ?";

        try (PreparedStatement ps = duckDBConnection.prepareStatement(sql)) {
            ps.setString(1, accountId);
            return executeAndMap(ps);
        } catch (SQLException e) {
            throw new IOException("DuckDB query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Run an arbitrary SQL query against the Iceberg table.
     * Useful for ad-hoc analytics (e.g., aggregations, GROUP BY).
     */
    public List<Transaction> queryTransactions(String whereClause) throws IOException {

        String sql = "SELECT * FROM iceberg_scan('" + icebergTablePath() + "') "
                + (whereClause != null && !whereClause.isBlank() ? "WHERE " + whereClause : "");

        try (Statement stmt = duckDBConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return mapResultSet(rs);
        } catch (SQLException e) {
            throw new IOException("DuckDB query failed: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /**
     * Build the S3 path to the Iceberg table metadata.
     * DuckDB's iceberg_scan reads the metadata directory directly from S3.
     */
    private String icebergTablePath() {
        // Ensure no double slashes
        String base = warehousePath.endsWith("/") ? warehousePath : warehousePath + "/";
        return base + namespace + "/" + tableName;
    }

    private List<Transaction> executeAndMap(PreparedStatement ps) throws SQLException {
        System.out.println("✓ DuckDB executing: " + ps);
        try (ResultSet rs = ps.executeQuery()) {
            List<Transaction> results = mapResultSet(rs);
            System.out.println("  → " + results.size() + " transaction(s) returned");
            return results;
        }
    }

    private List<Transaction> mapResultSet(ResultSet rs) throws SQLException {
        List<Transaction> results = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();

        while (rs.next()) {
            Transaction tx = new Transaction();
            tx.setTransactionId(getStringColumn(rs, meta, "transaction_id"));
            tx.setAccountId(getStringColumn(rs, meta, "account_id"));
            tx.setAmount(getDoubleColumn(rs, meta, "amount"));
            tx.setCurrency(getStringColumn(rs, meta, "currency"));
            tx.setTransactionType(getStringColumn(rs, meta, "transaction_type"));
            tx.setTimestamp(getLongColumn(rs, meta, "timestamp"));
            tx.setDescription(getStringColumn(rs, meta, "description"));

            // Partition columns may or may not be present in result set
            if (hasColumn(meta, "year"))  tx.setYear(rs.getInt("year"));
            if (hasColumn(meta, "month")) tx.setMonth(rs.getInt("month"));
            if (hasColumn(meta, "day"))   tx.setDay(rs.getInt("day"));

            results.add(tx);
        }
        return results;
    }

    // Safe column accessors -------------------------------------------

    private String getStringColumn(ResultSet rs, ResultSetMetaData meta, String col) throws SQLException {
        return hasColumn(meta, col) ? rs.getString(col) : null;
    }

    private double getDoubleColumn(ResultSet rs, ResultSetMetaData meta, String col) throws SQLException {
        return hasColumn(meta, col) ? rs.getDouble(col) : 0.0;
    }

    private long getLongColumn(ResultSet rs, ResultSetMetaData meta, String col) throws SQLException {
        return hasColumn(meta, col) ? rs.getLong(col) : 0L;
    }

    private boolean hasColumn(ResultSetMetaData meta, String columnName) throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }
}
