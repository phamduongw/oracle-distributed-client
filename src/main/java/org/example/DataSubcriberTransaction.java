package org.example;

import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSubcriberTransaction {

    private static final Logger LOGGER = Logger.getLogger(DataSubcriberTransaction.class.getName());

    private static String nextvalSql;
    private static String insertSql;
    private static String updateSql;
    private static String selectSql;

    private static String envOrDefault(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? defaultValue : v;
    }

    private static String loadResourceAsString(String resourcePath) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Resource not found: " + resourcePath);
            }
            return new String(is.readAllBytes());
        }
    }

    private static String buildUrl(String serviceName) {
        String host = envOrDefault("DB_HOST", null);
        String port = envOrDefault("DB_PORT", null);
        return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName;
    }

    private static PoolDataSource createCatalogPool() throws SQLException {
        String service = envOrDefault("CATALOG_SERVICE", null);
        String url = buildUrl(service);
        String user = envOrDefault("DB_USERNAME", null);
        String password = envOrDefault("DB_PASSWORD", null);

        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName(OracleDataSource.class.getName());
        pds.setURL(url);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setInitialPoolSize(1);
        pds.setMinPoolSize(1);
        pds.setMaxPoolSize(1);
        return pds;
    }

    private static PoolDataSource createAppPool() throws SQLException {
        String service = envOrDefault("APP_SERVICE", null);
        String url = buildUrl(service);
        String user = envOrDefault("DB_USERNAME", null);
        String password = envOrDefault("DB_PASSWORD", null);

        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName(OracleDataSource.class.getName());
        pds.setURL(url);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setInitialPoolSize(1);
        pds.setMinPoolSize(1);
        pds.setMaxPoolSize(1);
        return pds;
    }

    private static long getNextDataSubcriberId(PoolDataSource catalogPool) throws SQLException {
        try (Connection conn = catalogPool.getConnection(); PreparedStatement ps = conn.prepareStatement(nextvalSql); ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException("nextvalSql did not return a value");
            }
            return rs.getLong(1);
        }
    }

    private static String random11Digit() {
        long v = ThreadLocalRandom.current().nextLong(0, 100_000_000_000L);
        return String.format("%011d", v);
    }

    private static void executeTransactionWithRetry(PoolDataSource appPool, long id, String msisdn, String subId) {
        String threadName = Thread.currentThread().getName();
        while (true) {
            Connection conn = null;
            try {
                OracleShardingKey sk = appPool.createShardingKeyBuilder().subkey(id, JDBCType.NUMERIC).build();

                conn = appPool.createConnectionBuilder().shardingKey(sk).build();

                conn.setAutoCommit(false);

                try (PreparedStatement insPs = conn.prepareStatement(insertSql)) {
                    insPs.setLong(1, id);
                    insPs.setString(2, msisdn);
                    insPs.setString(3, subId);
                    insPs.executeUpdate();
                }

                try (PreparedStatement updPs = conn.prepareStatement(updateSql)) {
                    updPs.setString(1, msisdn);
                    updPs.setString(2, subId);
                    updPs.executeUpdate();
                }

                try (PreparedStatement selPs = conn.prepareStatement(selectSql)) {
                    selPs.setString(1, subId);
                    try (ResultSet rs = selPs.executeQuery()) {
                        while (rs.next()) {
                            rs.getLong(1);
                        }
                    }
                }

                conn.commit();
                LOGGER.info(() -> "Committed transaction [thread=" + threadName + ", ID=" + id + ", MSISDN=" + msisdn + ", SUB_ID=" + subId + "]");
                return;
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Transaction failed [thread=" + threadName + ", ID=" + id + ", MSISDN=" + msisdn + ", SUB_ID=" + subId + "], retrying", e);
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, "Rollback failed [thread=" + threadName + "]", ex);
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Unexpected error [thread=" + threadName + ", ID=" + id + ", MSISDN=" + msisdn + ", SUB_ID=" + subId + "], aborting transaction", e);
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, "Rollback failed [thread=" + threadName + "]", ex);
                    }
                }
                return;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, "Failed to close connection [thread=" + threadName + "]", ex);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            nextvalSql = loadResourceAsString("sql/nextval_data_subcriber.sql");
            insertSql = loadResourceAsString("sql/insert_data_subcriber.sql");
            updateSql = loadResourceAsString("sql/update_data_subcriber.sql");
            selectSql = loadResourceAsString("sql/select_data_subcriber.sql");

            PoolDataSource catalogPool = createCatalogPool();
            PoolDataSource appPool = createAppPool();

            String threadName = Thread.currentThread().getName();
            LOGGER.info("Starting single-thread load generator on thread " + threadName);

            while (true) {
                try {
                    long id = getNextDataSubcriberId(catalogPool);
                    String msisdn = random11Digit();
                    String subId = random11Digit();
                    executeTransactionWithRetry(appPool, id, msisdn, subId);
                } catch (SQLException e) {
                    LOGGER.log(Level.SEVERE, "Failed to get next id [thread=" + threadName + "], continuing", e);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start application", e);
        }
    }
}
