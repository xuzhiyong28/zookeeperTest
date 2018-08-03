package com.xzy.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @Auther: xuzy
 * @Date: 2018/8/3 15:37
 * @Description:
 */
public class ExclusiveLock implements Lock {
    private static final Logger logger = LoggerFactory.getLogger(ExclusiveLock.class);
    private static final String LOCK_NODE_FULL_PATH = "/exclusive_lock/lock";
    private static final long spinForTimeoutThreshold = 1000L; //超时的阀值
    private static final long SLEEP_TIME = 100L;
    private ZooKeeper zooKeeper;
    private LockStatus lockStatus;
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private CyclicBarrier lockBarrier = new CyclicBarrier(2);
    private String id = String.valueOf(new Random(System.nanoTime()).nextInt(10000000));

    public ExclusiveLock() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper("localhost:2181", 1000, new LockNodeWatcher());
        lockStatus = LockStatus.UNLOCK;
        connectedSemaphore.await();
    }

    @Override
    public void lock() {
        if (lockStatus != LockStatus.UNLOCK) {
            return;
        }
        //创建节点
        if (createLockNode()) {
            System.out.println("[" + id + "]获得锁");
            lockStatus = LockStatus.LOCKED;
            return;
        }
        lockStatus = LockStatus.TRY_LOCK;
        try {
            lockBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        if (lockStatus == LockStatus.LOCKED) {
            return true;
        }
        boolean created = createLockNode();
        lockStatus = created ? LockStatus.LOCKED : LockStatus.UNLOCK;
        return created;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long millisTimeout = time;
        if (millisTimeout <= 0L) {
            return false;
        }
        final long deadline = System.currentTimeMillis() + millisTimeout;
        while (true) {
            if (tryLock()) {
                return true;
            }
            if (millisTimeout > spinForTimeoutThreshold) {
                Thread.sleep(SLEEP_TIME);
            }
            millisTimeout = deadline - System.currentTimeMillis();
            if (millisTimeout <= 0L) {
                return false;
            }
        }
    }

    @Override
    public void unlock() {
        if (lockStatus == LockStatus.UNLOCK) {
            return;
        }
        try {
            deleteLockNode();
            lockStatus = LockStatus.UNLOCK;
            lockBarrier.reset();
            System.out.println("[" + id + "]" + " 释放锁");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }


    /***
     * 创建节点
     * @return
     */
    private Boolean createLockNode() {
        try {
            zooKeeper.create(LOCK_NODE_FULL_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /***
     * 刪除節點
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void deleteLockNode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(LOCK_NODE_FULL_PATH, false);
        zooKeeper.delete(LOCK_NODE_FULL_PATH, stat.getVersion());
    }


    class LockNodeWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected != event.getState()) {
                return;
            }
            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                connectedSemaphore.countDown();
            }
        }
    }
}
