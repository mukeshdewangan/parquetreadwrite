package com.mukesh.coldstorageexp.parquetreadwrite.config;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class IcebergCatalogConfig {

    @Value("${iceberg.catalog.uri}")
    private String catalogUri;

    @Value("${iceberg.catalog.warehouse}")
    private String warehousePath;

    @Value("${iceberg.catalog.name:iceberg}")
    private String catalogName;

    @Value("${iceberg.catalog.db.user:postgres}")
    private String dbUser;

    @Value("${iceberg.catalog.db.password:postgres}")
    private String dbPassword;

    @Value("${iceberg.s3.region:eu-north-1}")
    private String s3Region;

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        
        // Register S3A filesystem explicitly
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        // AWS credentials provider
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        
        // S3A configuration
        conf.set("fs.s3a.region", s3Region);
        conf.set("fs.s3a.endpoint.region", s3Region);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.maximum", "100");
        
        // Debug logging
        System.out.println("✓ Hadoop S3A Configuration:");
        System.out.println("  Region: " + s3Region);
        System.out.println("  Credentials Provider: DefaultAWSCredentialsProviderChain");
        
        return conf;
    }

    @Bean
    public Catalog icebergCatalog(org.apache.hadoop.conf.Configuration hadoopConfig) {
        JdbcCatalog catalog = new JdbcCatalog();
        
        Map<String, String> properties = new HashMap<>();
        // PostgreSQL JDBC connection
        properties.put("uri", catalogUri);
        properties.put("jdbc.user", dbUser);
        properties.put("jdbc.password", dbPassword);
        
        // S3 warehouse location
        properties.put("warehouse", warehousePath);
        
        // AWS S3 configuration for Iceberg
        properties.put("s3.region", s3Region);
        properties.put("s3.endpoint.region", s3Region);
        
        // Pass Hadoop configuration explicitly
        catalog.setConf(hadoopConfig);
        
        catalog.initialize(catalogName, properties);
        
        System.out.println("✓ Initialized Iceberg JDBC Catalog");
        System.out.println("  Metadata Database: " + catalogUri);
        System.out.println("  Warehouse Location: " + warehousePath);
        System.out.println("  AWS Region: " + s3Region);
        
        return catalog;
    }
}
