package vn.bnh.benchmark;

import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.Connection;
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

public class DataSubcriberTransactionRac {

    private static final Logger LOGGER = Logger.getLogger(DataSubcriberTransactionRac.class.getName());

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

    private static String buildRacUrl() {
        String host = env("RAC_HOST");
        String port = env("RAC_PORT");
        String service = env("RAC_APP_SERVICE");
        return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
    }

    private static PoolDataSource createRacPool() throws SQLException {
        String url = buildRacUrl();
        String user = env("RAC_USERNAME");
        String password = env("RAC_PASSWORD");

        int initialSize = envInt("APP_INITIAL_POOL_SIZE");
        int minSize = envInt("APP_MIN_POOL_SIZE");
        int maxSize = envInt("APP_MAX_POOL_SIZE");

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

    private static long getNextId(PoolDataSource pool) throws SQLException {
        try (Connection c = pool.getConnection(); PreparedStatement ps = c.prepareStatement(seqSql); ResultSet rs = ps.executeQuery()) {

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

    private static String threadLabel() {
        return "[thread=" + Thread.currentThread().getName() + "] ";
    }

    private static void executeTransaction(PoolDataSource pool, long id, String msisdn, String subId) throws SQLException {
        try (Connection conn = pool.getConnection()) {

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
        }
    }

    private static ExecutorService createExecutor(int threads) {
        AtomicInteger idx = new AtomicInteger(1);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("worker-rac-" + idx.getAndIncrement());
            return t;
        };
        return Executors.newFixedThreadPool(threads, tf);
    }

    private static void startWorkers(ExecutorService executor, int threadCount, PoolDataSource pool) {
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                LOGGER.info(threadLabel() + "Worker started");
                while (true) {
                    try {
                        long id = getNextId(pool);
                        String msisdn = random11Digit();
                        String subId = random11Digit();
                        executeTransaction(pool, id, msisdn, subId);
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

            PoolDataSource pool = createRacPool();

            LOGGER.info("DataSubcriberTransactionRac STARTED with threads=" + threadCount + ", MAX_ID=" + maxId);

            ExecutorService executor = createExecutor(threadCount);
            startWorkers(executor, threadCount, pool);

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
