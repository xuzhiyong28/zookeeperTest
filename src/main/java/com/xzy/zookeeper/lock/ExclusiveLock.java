package com.xzy.zookeeper.lock;

import org.apache.zookeeper.*;
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
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {

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
