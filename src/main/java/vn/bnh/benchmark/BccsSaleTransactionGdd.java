package vn.bnh.benchmark;

import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BccsSaleTransactionGdd {

    private static final Logger LOGGER = Logger.getLogger(BccsSaleTransactionGdd.class.getName());

    private static String customerSql;
    private static String custIdentitySql;
    private static String accountSql;
    private static String subscriberSql;

    private static long maxTransactions;
    private static final AtomicLong SUCCESS_COUNT = new AtomicLong(0);
    private static final Object COUNTER_LOCK = new Object();

    private static String env(String key) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException("Missing env: " + key);
        }
        return v;
    }

    private static int envInt(String key) {
        return Integer.parseInt(env(key));
    }

    private static long envLong(String key) {
        return Long.parseLong(env(key));
    }

    private static String loadSql(String path) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("Missing SQL file: " + path);
            }
            return new String(is.readAllBytes());
        }
    }

    private static String buildUrl() {
        return "jdbc:oracle:thin:@//" + env("GDD_HOST") + ":" + env("GDD_PORT") + "/" + env("GDD_APP_SERVICE");
    }

    private static PoolDataSource createAppPool() throws SQLException {
        PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
        p.setConnectionFactoryClassName(OracleDataSource.class.getName());
        p.setURL(buildUrl());
        p.setUser(env("GDD_USERNAME"));
        p.setPassword(env("GDD_PASSWORD"));
        p.setInitialPoolSize(envInt("APP_INITIAL_POOL_SIZE"));
        p.setMinPoolSize(envInt("APP_MIN_POOL_SIZE"));
        p.setMaxPoolSize(envInt("APP_MAX_POOL_SIZE"));
        return p;
    }

    private static boolean isOra3838(SQLException e) {
        if (e.getErrorCode() == 3838) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && msg.contains("ORA-03838");
    }

    private static OracleShardingKey createShardingKey(PoolDataSource pool, String custId) throws SQLException {
        return pool.createShardingKeyBuilder().subkey(custId, JDBCType.VARCHAR).build();
    }

    /**
     * Thực hiện 1 transaction.
     *
     * @return true nếu commit thành công và vẫn còn quota; false nếu đã đủ MAX_ID và thread nên dừng.
     */
    private static boolean executeTransaction(PoolDataSource pool) {

        // Nếu đã đủ quota thì không làm gì nữa
        if (SUCCESS_COUNT.get() >= maxTransactions) {
            return false;
        }

        String custId = UUID.randomUUID().toString();
        String custIdentityId = UUID.randomUUID().toString();
        String accountId = UUID.randomUUID().toString();
        String subId = UUID.randomUUID().toString();

        for (; ; ) {
            try (Connection conn = pool.createConnectionBuilder().shardingKey(createShardingKey(pool, custId)).build()) {

                conn.setAutoCommit(false);

                try (PreparedStatement ps = conn.prepareStatement(customerSql)) {
                    ps.setString(1, custId);
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(custIdentitySql)) {
                    ps.setString(1, custIdentityId);
                    ps.setString(2, custId);
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(accountSql)) {
                    ps.setString(1, accountId);
                    ps.setString(2, custId);
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(subscriberSql)) {
                    ps.setString(1, subId);     // SUB_ID
                    ps.setString(2, subId);     // CONTRACT_ID
                    ps.setString(3, custId);    // CUST_ID
                    ps.setString(4, accountId); // ACCOUNT_ID
                    ps.executeUpdate();
                }

                // Serialize: check quota + commit + tăng counter
                synchronized (COUNTER_LOCK) {
                    if (SUCCESS_COUNT.get() >= maxTransactions) {
                        conn.rollback();
                        return false;
                    }

                    conn.commit();
                    long done = SUCCESS_COUNT.incrementAndGet();
                    LOGGER.info("[" + Thread.currentThread().getName() + "] COMMIT CUST_ID=" + custId + " SUB_ID=" + subId + " TOTAL=" + done);

                    if (done >= maxTransactions) {
                        return false;
                    }
                }

                return true;
            } catch (SQLException e) {
                if (isOra3838(e)) {
                    LOGGER.warning("[" + Thread.currentThread().getName() + "] ORA-03838 retry CUST_ID=" + custId + " SUB_ID=" + subId);
                    continue;
                }
                LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] SQL ERROR (STOP) CUST_ID=" + custId + " SUB_ID=" + subId, e);
                System.exit(1);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] UNEXPECTED ERROR (STOP) CUST_ID=" + custId + " SUB_ID=" + subId, e);
                System.exit(1);
            }
        }
    }

    private static ExecutorService createExecutor(int threads) {
        AtomicInteger idx = new AtomicInteger(1);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("bccs-gdd-" + idx.getAndIncrement());
            return t;
        };
        return Executors.newFixedThreadPool(threads, tf);
    }

    public static void main(String[] args) {
        try {
            customerSql = loadSql("sql/insert_customer.sql");
            custIdentitySql = loadSql("sql/insert_cust_identity.sql");
            accountSql = loadSql("sql/insert_account.sql");
            subscriberSql = loadSql("sql/insert_subscriber.sql");

            int threads = envInt("THREAD_COUNT");
            maxTransactions = envLong("MAX_ID");

            PoolDataSource pool = createAppPool();

            LOGGER.info("BccsSaleTransactionGdd STARTED with threads=" + threads + ", MAX_ID=" + maxTransactions);

            ExecutorService executor = createExecutor(threads);
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    while (executeTransaction(pool)) {
                        // loop đến khi hết quota hoặc có lỗi fatal (System.exit)
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

            LOGGER.info("BccsSaleTransactionGdd FINISHED, TOTAL_SUCCESS=" + SUCCESS_COUNT.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
            System.exit(1);
        }
    }
}
