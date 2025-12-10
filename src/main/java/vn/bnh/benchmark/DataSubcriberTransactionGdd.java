package vn.bnh.benchmark;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSubcriberTransactionGdd {

    private static final Logger LOGGER = Logger.getLogger(DataSubcriberTransactionGdd.class.getName());

    private static String seqSql;
    private static String insertSql;
    private static String updateSql;
    private static String selectSql;

    private static long maxId;

    private static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return v;
    }

    private static int envInt(String name) {
        return Integer.parseInt(env(name));
    }

    private static long envLong(String name) {
        return Long.parseLong(env(name));
    }

    private static String load(String resourcePath) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Missing resource: " + resourcePath);
            }
            return new String(is.readAllBytes());
        }
    }

    private static String buildGddUrl(String serviceEnvKey) {
        String host = env("GDD_HOST");
        String port = env("GDD_PORT");
        String service = env(serviceEnvKey);
        return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
    }

    private static PoolDataSource createGddPool(String serviceEnvKey, int initialSize, int minSize, int maxSize) throws SQLException {
        String url = buildGddUrl(serviceEnvKey);
        String user = env("GDD_USERNAME");
        String password = env("GDD_PASSWORD");

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

    private static PoolDataSource createCatalogPool() throws SQLException {
        int initialSize = envInt("CATALOG_INITIAL_POOL_SIZE");
        int minSize = envInt("CATALOG_MIN_POOL_SIZE");
        int maxSize = envInt("CATALOG_MAX_POOL_SIZE");
        return createGddPool("GDD_CATALOG_SERVICE", initialSize, minSize, maxSize);
    }

    private static PoolDataSource createAppPool() throws SQLException {
        int initialSize = envInt("APP_INITIAL_POOL_SIZE");
        int minSize = envInt("APP_MIN_POOL_SIZE");
        int maxSize = envInt("APP_MAX_POOL_SIZE");
        return createGddPool("GDD_APP_SERVICE", initialSize, minSize, maxSize);
    }

    private static long getNextId(PoolDataSource catalogPool) throws SQLException {
        try (Connection c = catalogPool.getConnection(); PreparedStatement ps = c.prepareStatement(seqSql); ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException("Sequence did not return value");
            }

            long id = rs.getLong(1);

            if (id > maxId) {
                LOGGER.info("Sequence value " + id + " exceeded MAX_ID=" + maxId + " -> stopping program.");
                System.exit(0);
            }

            return id;
        }
    }

    private static String random11Digit() {
        long v = ThreadLocalRandom.current().nextLong(0, 100_000_000_000L);
        return String.format("%011d", v);
    }

    private static boolean isOra3838(SQLException e) {
        for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
            if (cur.getErrorCode() == 3838) {
                return true;
            }
            String msg = cur.getMessage();
            if (msg != null && msg.contains("ORA-03838")) {
                return true;
            }
        }
        return false;
    }

    private static String threadLabel() {
        return "[thread=" + Thread.currentThread().getName() + "] ";
    }

    private static OracleShardingKey createShardingKey(PoolDataSource appPool, long id) throws SQLException {
        return appPool.createShardingKeyBuilder().subkey(id, JDBCType.NUMERIC).build();
    }

    private static void executeTransactionWithRetry(PoolDataSource appPool, long id, String msisdn, String subId) {
        for (; ; ) {
            try (Connection conn = appPool.createConnectionBuilder().shardingKey(createShardingKey(appPool, id)).build()) {

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
                if (isOra3838(e)) {
                    LOGGER.warning(threadLabel() + "ORA-3838, retry with new leader, id=" + id);
                    continue;
                }
                LOGGER.log(Level.SEVERE, threadLabel() + "Unexpected SQL error for id=" + id, e);
                return;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, threadLabel() + "Unexpected error for id=" + id, e);
                return;
            }
        }
    }

    private static ExecutorService createExecutor(int threads) {
        AtomicInteger idx = new AtomicInteger(1);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("worker-gdd-" + idx.getAndIncrement());
            return t;
        };
        return Executors.newFixedThreadPool(threads, tf);
    }

    private static void startWorkers(ExecutorService executor, int threadCount, PoolDataSource catalogPool, PoolDataSource appPool) {
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
    }

    public static void main(String[] args) {
        try {
            seqSql = load("sql/nextval_data_subcriber.sql");
            insertSql = load("sql/insert_data_subcriber.sql");
            updateSql = load("sql/update_data_subcriber.sql");
            selectSql = load("sql/select_data_subcriber.sql");

            int threadCount = envInt("THREAD_COUNT");
            maxId = envLong("MAX_ID");

            PoolDataSource catalogPool = createCatalogPool();
            PoolDataSource appPool = createAppPool();

            LOGGER.info("DataSubcriberTransactionGdd STARTED with threads=" + threadCount + ", MAX_ID=" + maxId);

            ExecutorService executor = createExecutor(threadCount);
            startWorkers(executor, threadCount, catalogPool, appPool);

            executor.shutdown();
            boolean terminated = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            if (!terminated) {
                LOGGER.warning("Executor did not terminate within the specified timeout");
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
        }
    }
}
