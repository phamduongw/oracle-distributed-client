package org.example;

import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSubcriberTransaction {

    private static final Logger LOGGER = Logger.getLogger(DataSubcriberTransaction.class.getName());

    private static String seqSql;
    private static String insertSql;
    private static String updateSql;
    private static String selectSql;

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? def : v;
    }

    private static int envInt(String name, String def) {
        return Integer.parseInt(env(name, def));
    }

    private static String load(String resourcePath) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Missing: " + resourcePath);
            }
            return new String(is.readAllBytes());
        }
    }

    private static String buildUrl(String serviceName) {
        String host = env("DB_HOST", "10.10.11.147");
        String port = env("DB_PORT", "1522");
        return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName;
    }

    private static PoolDataSource createCatalogPool() throws SQLException {
        String service = env("CATALOG_SERVICE", "GDS$CATALOG.raft_vmdb");
        String url = buildUrl(service);
        String user = env("DB_USERNAME", "app_schema");
        String password = env("DB_PASSWORD", "App_Schema_Pass_123");

        int initialSize = envInt("CATALOG_INITIAL_POOL_SIZE", "1");
        int minSize = envInt("CATALOG_MIN_POOL_SIZE", "1");
        int maxSize = envInt("CATALOG_MAX_POOL_SIZE", "5");

        PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
        p.setConnectionFactoryClassName(OracleDataSource.class.getName());
        p.setURL(url);
        p.setUser(user);
        p.setPassword(password);
        p.setInitialPoolSize(initialSize);
        p.setMinPoolSize(minSize);
        p.setMaxPoolSize(maxSize);
        return p;
    }

    private static PoolDataSource createAppPool() throws SQLException {
        String service = env("APP_SERVICE", "app_rw_svc.oak.raft_vmdb");
        String url = buildUrl(service);
        String user = env("DB_USERNAME", "app_schema");
        String password = env("DB_PASSWORD", "App_Schema_Pass_123");

        int initialSize = envInt("APP_INITIAL_POOL_SIZE", "5");
        int minSize = envInt("APP_MIN_POOL_SIZE", "5");
        int maxSize = envInt("APP_MAX_POOL_SIZE", "5");

        PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
        p.setConnectionFactoryClassName(OracleDataSource.class.getName());
        p.setURL(url);
        p.setUser(user);
        p.setPassword(password);
        p.setInitialPoolSize(initialSize);
        p.setMinPoolSize(minSize);
        p.setMaxPoolSize(maxSize);
        return p;
    }

    private static long getNextId(PoolDataSource catalogPool) throws SQLException {
        try (Connection c = catalogPool.getConnection(); PreparedStatement ps = c.prepareStatement(seqSql); ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException("Sequence did not return value");
            }
            return rs.getLong(1);
        }
    }

    private static String random11Digit() {
        long v = ThreadLocalRandom.current().nextLong(0, 100_000_000_000L);
        return String.format("%011d", v);
    }

    private static boolean isOra3838(SQLException e) {
        SQLException cur = e;
        while (cur != null) {
            if (cur.getErrorCode() == 3838) {
                return true;
            }
            String msg = cur.getMessage();
            if (msg != null && msg.contains("ORA-03838")) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    private static String threadLabel() {
        return "[thread=" + Thread.currentThread().getName() + "] ";
    }

    private static void executeTransactionWithRetry(PoolDataSource appPool, long id, String msisdn, String subId) {
        for (; ; ) {
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
                LOGGER.info(threadLabel() + "COMMIT id=" + id + " msisdn=" + msisdn + " subId=" + subId);
                return;
            } catch (SQLException e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, threadLabel() + "Rollback failed for id=" + id, ex);
                    }
                }

                if (isOra3838(e)) {
                    LOGGER.warning(threadLabel() + "ORA-3838, retry with new leader, id=" + id);
                    continue;
                }

                LOGGER.log(Level.SEVERE, threadLabel() + "Unexpected SQL error for id=" + id, e);
                return;
            } catch (Exception e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, threadLabel() + "Rollback failed for id=" + id, ex);
                    }
                }
                LOGGER.log(Level.SEVERE, threadLabel() + "Unexpected error for id=" + id, e);
                return;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        LOGGER.log(Level.WARNING, threadLabel() + "Failed to close connection for id=" + id, ex);
                    }
                }
            }
        }
    }

    private static ExecutorService createExecutor(int threads) {
        ThreadFactory tf = new ThreadFactory() {
            private int idx = 1;

            @Override
            public synchronized Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("worker-" + idx++);
                return t;
            }
        };
        return Executors.newFixedThreadPool(threads, tf);
    }

    public static void main(String[] args) {
        try {
            seqSql = load("sql/nextval_data_subcriber.sql");
            insertSql = load("sql/insert_data_subcriber.sql");
            updateSql = load("sql/update_data_subcriber.sql");
            selectSql = load("sql/select_data_subcriber.sql");

            int threadCount = envInt("THREAD_COUNT", "5");

            PoolDataSource catalogPool = createCatalogPool();
            PoolDataSource appPool = createAppPool();

            LOGGER.info("DataSubcriberTransaction STARTED with threads=" + threadCount);

            ExecutorService executor = createExecutor(threadCount);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    LOGGER.info(threadLabel() + "Worker started");
                    while (true) {
                        try {
                            long id = getNextId(catalogPool);
                            String msisdn = random11Digit();
                            String subId = random11Digit();
                            executeTransactionWithRetry(appPool, id, msisdn, subId);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, threadLabel() + "Worker error", e);
                        }
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
        }
    }
}
