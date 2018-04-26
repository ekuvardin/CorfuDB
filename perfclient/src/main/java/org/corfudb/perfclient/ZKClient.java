package org.corfudb.perfclient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Created by m on 4/26/18.
 */
public class ZKClient {

    public static void create(String path, byte[] data, ZooKeeper zk) throws
            KeeperException,InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    public static ZooKeeper getConnection() throws Exception {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("127.0.0.1", 2181,new Watcher() {

            public void process(WatchedEvent we) {

                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
        return zk;
    }

    public static void main(String[] args) throws Exception  {


        final int opsPerThread = 20000;
        final String prefix = "/";
        final int payloadSize = 1000;

        Runnable r1 = () -> {
            try {
                ZooKeeper conn = getConnection();
                byte[] payload = new byte[payloadSize];

                for (int x = 0; x < opsPerThread; x++) {
                    UUID id = UUID.randomUUID();
                    create(prefix+id.toString(), payload, conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable r2 = () -> {
            try {
                ZooKeeper conn = getConnection();
                byte[] payload = new byte[payloadSize];

                for (int x = 0; x < opsPerThread; x++) {
                    UUID id = UUID.randomUUID();
                    create(prefix+id.toString(), payload, conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable r3 = () -> {
            try {
                ZooKeeper conn = getConnection();
                byte[] payload = new byte[payloadSize];

                for (int x = 0; x < opsPerThread; x++) {
                    UUID id = UUID.randomUUID();
                    create(prefix+id.toString(), payload, conn);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);

        long s1 = System.currentTimeMillis();
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        long s2 = System.currentTimeMillis();

        System.out.println("time " + (s2 - s1));
        System.out.println("throughput " + ((20_000 * 3.0)/(s2-s1)));



    }
}
