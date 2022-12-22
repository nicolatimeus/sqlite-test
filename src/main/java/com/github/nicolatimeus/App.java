package com.github.nicolatimeus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.sqlite.javax.SQLiteConnectionPoolDataSource;

/**
 * Hello world!
 *
 */
public class App {

    private static class DummyConnectionPool implements ConnectionEventListener {
        private final List<PooledConnection> connections = new ArrayList<>();
        private final ConnectionPoolDataSource dataSource;

        public DummyConnectionPool(final ConnectionPoolDataSource dataSource) {
            this.dataSource = dataSource;
        }

        public synchronized Connection getConnection() throws SQLException {
            final Iterator<PooledConnection> iter = connections.iterator();

            final PooledConnection result;

            if (iter.hasNext()) {
                result = iter.next();
                iter.remove();
                System.out.println("reusing connection " + result);
            } else {
                result = dataSource.getPooledConnection();
                result.addConnectionEventListener(this);
                System.out.println("new connection opened" + result);
            }

            return result.getConnection();
        }

        @Override
        public synchronized void connectionClosed(ConnectionEvent event) {

            final PooledConnection connection = (PooledConnection) event.getSource();
            System.out.println("connection released" + connection);
            connections.add(connection);
        }

        @Override
        public void connectionErrorOccurred(ConnectionEvent event) {
            System.out.println("connection error");
            event.getSQLException().printStackTrace();
        }

    }

    private static void worker(final DummyConnectionPool connectionPool, final int iterations,
            final AtomicBoolean abort) {
        for (int i = 0; i < iterations && !abort.get(); i++) {
            try {
                try (final Connection connection = connectionPool.getConnection()) {
                    connection.setAutoCommit(false);
                }
            } catch (final Exception e) {
                abort.set(true);
                System.out.println("worker error");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("starting test");

        final SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
        dataSource.setUrl("jdbc:sqlite:sample.db");

        final DummyConnectionPool connectionPool = new DummyConnectionPool(dataSource);

        final List<Thread> threads = new ArrayList<>();

        final AtomicBoolean abort = new AtomicBoolean(false);

        for (int i = 0; i < 200; i++) {
            final Thread thread = new Thread(() -> worker(connectionPool, 200000, abort));
            thread.start();
            threads.add(thread);
        }

        for (final Thread t : threads) {
            t.join();
        }
    }

}
