package vn.bnh.benchmark;

import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BccsSaleTransactionRac {

    private static final Logger LOGGER = Logger.getLogger(BccsSaleTransactionRac.class.getName());

    private static String customerSql;
    private static String custIdentitySql;
    private static String accountSql;
    private static String subscriberSql;

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

    private static String loadSql(String path) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("Missing SQL file: " + path);
            }
            return new String(is.readAllBytes());
        }
    }

    private static String buildUrl() {
        return "jdbc:oracle:thin:@//" + env("RAC_HOST") + ":" + env("RAC_PORT") + "/" + env("RAC_APP_SERVICE");
    }

    private static PoolDataSource createAppPool() throws SQLException {
        PoolDataSource p = PoolDataSourceFactory.getPoolDataSource();
        p.setConnectionFactoryClassName(OracleDataSource.class.getName());
        p.setURL(buildUrl());
        p.setUser(env("RAC_USERNAME"));
        p.setPassword(env("RAC_PASSWORD"));
        p.setInitialPoolSize(envInt("APP_INITIAL_POOL_SIZE"));
        p.setMinPoolSize(envInt("APP_MIN_POOL_SIZE"));
        p.setMaxPoolSize(envInt("APP_MAX_POOL_SIZE"));
        return p;
    }

    private static void executeTransaction(PoolDataSource pool) {

        String custId = UUID.randomUUID().toString();
        String custIdentityId = UUID.randomUUID().toString();
        String accountId = UUID.randomUUID().toString();
        String subId = UUID.randomUUID().toString();

        for (; ; ) {
            try (Connection conn = pool.getConnection()) {

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
                    ps.setString(2, subId);     // CONTRACT_ID (cùng SUB_ID)
                    ps.setString(3, custId);    // CUST_ID
                    ps.setString(4, accountId); // ACCOUNT_ID
                    ps.executeUpdate();
                }

                conn.commit();
                LOGGER.info("[" + Thread.currentThread().getName() + "] COMMIT CUST_ID=" + custId + " SUB_ID=" + subId);
                return;
            } catch (SQLException e) {
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
            t.setName("bccs-rac-" + idx.getAndIncrement());
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
            PoolDataSource pool = createAppPool();

            LOGGER.info("BccsSaleTransactionRac STARTED with threads=" + threads);

            ExecutorService executor = createExecutor(threads);
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    while (true) {
                        executeTransaction(pool);
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
            System.exit(1);
        }
    }
}
