package vn.bnh.benchmark;

import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BccsSaleQpsGdd {

    private static final Logger LOGGER = Logger.getLogger(BccsSaleQpsGdd.class.getName());

    private static String qpsSql;

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
        p.setConnectionProperty("oracle.jdbc.useShardingDriverConnection", "true");
        return p;
    }

    private static String randomIdNo12() {
        long v = ThreadLocalRandom.current().nextLong(0, 1_000_000_000_000L);
        return String.format("%012d", v);
    }

    private static ExecutorService createExecutor(int threads, String prefix) {
        AtomicInteger idx = new AtomicInteger(1);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName(prefix + "-" + idx.getAndIncrement());
            return t;
        };
        return Executors.newFixedThreadPool(threads, tf);
    }

    public static void main(String[] args) {
        try {
            qpsSql = loadSql("sql/qps_select_subscriber_by_idno.sql");

            int threads = envInt("THREAD_COUNT");
            PoolDataSource pool = createAppPool();

            LOGGER.info("BccsSaleQpsGdd STARTED with threads=" + threads);

            AtomicLong totalQueries = new AtomicLong(0);

            ExecutorService workers = createExecutor(threads, "bccs-qps");
            for (int i = 0; i < threads; i++) {
                workers.submit(() -> {
                    try (Connection conn = pool.getConnection(); PreparedStatement ps = conn.prepareStatement(qpsSql)) {

                        while (true) {
                            String idNo = randomIdNo12();
                            ps.setString(1, idNo);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) {
                                    rs.getLong(1);
                                }
                            }
                            totalQueries.incrementAndGet();
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "[" + Thread.currentThread().getName() + "] Worker ERROR, stopping program", e);
                        System.exit(1);
                    }
                });
            }

            ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("bccs-qps-monitor");
                return t;
            });

            AtomicLong lastTotal = new AtomicLong(0);
            monitor.scheduleAtFixedRate(() -> {
                long current = totalQueries.get();
                long last = lastTotal.getAndSet(current);
                long qps = current - last;
                LOGGER.info("QPS = " + qps + " queries/sec (total=" + current + ")");
            }, 1, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "FATAL", e);
            System.exit(1);
        }
    }
}
