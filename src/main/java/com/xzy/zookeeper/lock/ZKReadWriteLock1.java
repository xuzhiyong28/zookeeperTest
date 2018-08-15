package com.xzy.zookeeper.lock;/**
 * Created by Administrator on 2018-08-04.
 */

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * @author xuzhiyong
 * @createDate 2018-08-04-16:39
 */
public class ZKReadWriteLock1 implements ReadWriteLock {


    private static final String LOCK_NODE_PARENT_PATH = "/share_lock";
    private static final long spinForTimeoutThreshold = 1000L;
    private static final long SLEEP_TIME = 100L;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private Comparator<String> nameComparator;
    private ReadLock readLock = new ReadLock();

    public ZKReadWriteLock1() throws Exception {
        zooKeeper = new ZooKeeper("localhost:2181", 600000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(Event.KeeperState.SyncConnected == watchedEvent.getState()){
                    connectedSemaphore.countDown();
                }
            }
        });
        connectedSemaphore.await();
        System.out.println("====zookeeper====连接成功");
        nameComparator = new Comparator<String>() {
            @Override
            public int compare(String x, String y) {
                Integer xs = getSequence(x);
                Integer ys = getSequence(y);
                return xs > ys ? 1 : (xs < ys ? -1 : 0);
            }
        };

    }

    private Integer getSequence(String name) {
        return Integer.valueOf(name.substring(name.lastIndexOf("-") + 1));
    }

    @Override
    public DistributedLock readLock() {
        return new ReadLock();
    }

    @Override
    public DistributedLock writeLock() {
        return null;
    }

    /***
     * 创建节点
     * @param name
     * @return
     */
    private String createLockNode(String name){
        String path = null;
        try {
            path = zooKeeper.create(LOCK_NODE_PARENT_PATH + "/" + name , "".getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(" failed to create lock node");
            return null;
        }
        return path;
    }

    public void deleteLockNode(String name) throws Exception{
        Stat stat = zooKeeper.exists(LOCK_NODE_PARENT_PATH + "/" + name , false);
        zooKeeper.delete(LOCK_NODE_PARENT_PATH + "/" + name , stat.getVersion());
    }

    /***
     * 如果是第一个节点，直接返回true.
     * 如果不是，判断前面的节点是否是读节点，则可以加锁，不是的话就不能
     * @param name
     * @param nodes
     * @return
     */
    private Boolean canAcquireLock(String name, List<String> nodes){
        if(isFirstNode(name,nodes)){
            return true;
        }
        Map<String , Boolean> map = new HashMap<>();
        boolean hasWriteoperation = false;
        for(String n : nodes){
            if(n.contains("read") && hasWriteoperation == false){
                map.put(n,true);
            }else{
                hasWriteoperation = true;
                map.put(n,false);
            }
        }
        return map.get(name);
    }

    /***
     * 判断是不是第一个节点
     * @param name
     * @param nodes
     * @return
     */
    private boolean isFirstNode(String name, List<String> nodes) {
        return nodes.get(0).equals(name);
    }


    /***
     * 读锁
     */
    private class ReadLock implements DistributedLock , Watcher{

        private LockStatus lockStatus = LockStatus.UNLOCK;
        private CyclicBarrier lockBarrier = new CyclicBarrier(2);
        private String prefix = new Random(System.nanoTime()).nextInt(10000000) + "-read-";
        private String name;

        @Override
        public void lock() throws Exception {
            if(lockStatus != LockStatus.UNLOCK){
                return ;
            }
            if(name == null){
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }
            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this); //获取锁节点列表并设置监听
            nodes.sort(nameComparator); //进行排序
            //检查是否能获取到锁，如果能则直接返回
            if(canAcquireLock(name,nodes)){
                System.out.println(name + " 获取锁");
                lockStatus = LockStatus.LOCKED;
                return;
            }
            lockStatus = LockStatus.TRY_LOCK;
            lockBarrier.await();
        }

        @Override
        public Boolean tryLock() throws Exception {
            if(lockStatus == LockStatus.LOCKED){
                return true;
            }
            if(name == null){
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }
            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH , this);
            nodes.sort(nameComparator);
            if(canAcquireLock(name , nodes)){
                lockStatus = LockStatus.LOCKED;
                try{
                    lockBarrier.await();
                    System.out.println(name + " 获取锁");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            return null;
        }

        @Override
        public Boolean tryLock(long millisecond) throws Exception {
            long millisTimeout = millisecond;
            if(millisTimeout <= 0L){
                return false;
            }
            final long deadline = System.currentTimeMillis() + millisTimeout;
            while(true){
                if(tryLock()){
                    return true;
                }
                if(millisTimeout > spinForTimeoutThreshold){
                    Thread.sleep(SLEEP_TIME);
                }
                millisTimeout = deadline - System.currentTimeMillis();
                if (millisTimeout <= 0L) {
                    return false;
                }
            }
        }

        @Override
        public void unlock() throws Exception {
            if (lockStatus == LockStatus.UNLOCK) {
                return;
            }

            deleteLockNode(name);
            lockStatus = LockStatus.UNLOCK;
            lockBarrier.reset();
            System.out.println(name + " 释放锁");
            name = null;
        }

        @Override
        public void process(WatchedEvent event) {
            if(Event.KeeperState.SyncConnected != event.getState()){
                return ;
            }
            if(Event.EventType.None == event.getType() && event.getPath() == null){
                connectedSemaphore.countDown();
            }
            else if(Event.EventType.NodeChildrenChanged == event.getType() && event.getPath().equals(LOCK_NODE_PARENT_PATH)){
                if(lockStatus != LockStatus.TRY_LOCK){
                    return;
                }
                List<String> nodes = null;
                try{
                    nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH , this);
                    nodes.sort(nameComparator);
                }catch (Exception e){
                    e.printStackTrace();
                    return;
                }
                // 判断前面是否有写操作 ? 获取锁 ：等待
                if (canAcquireLock(name, nodes)) {
                    lockStatus = LockStatus.LOCKED;
                    try{
                        lockBarrier.await();
                        System.out.println(name + " 获取锁");
                    }catch (Exception e){

                    }
                }

            }
        }
    }

    /***
     * 写锁
     */
    private class WriteLock implements DistributedLock, Watcher{

        private LockStatus lockStatus = LockStatus.UNLOCK;
        private CyclicBarrier lockBarrier = new CyclicBarrier(2);
        private String prefix = new Random(System.nanoTime()).nextInt(1000000) + "-write-";
        private String name;

        @Override
        public void lock() throws Exception {
            if(lockStatus != LockStatus.UNLOCK){
                return;
            }
            if(name == null){
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }
            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH , this);
            nodes.sort(nameComparator);
            if (isFirstNode(name, nodes)) {
                System.out.println(name + " 获取锁");
                lockStatus = LockStatus.LOCKED;
                return;
            }
            lockStatus = LockStatus.TRY_LOCK;
            lockBarrier.await();
        }

        @Override
        public Boolean tryLock() throws Exception {
            if (lockStatus == LockStatus.LOCKED) {
                return true;
            }

            if (name == null) {
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }

            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
            nodes.sort(nameComparator);

            if (isFirstNode(name, nodes)) {
                lockStatus = LockStatus.LOCKED;
                return true;
            }

            return false;
        }

        @Override
        public Boolean tryLock(long millisecond) throws Exception {
            long millisTimeout = millisecond;
            if (millisTimeout <= 0L) {
                return false;
            }

            final long deadline = System.currentTimeMillis() + millisTimeout;
            while(true) {
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
        public void unlock() throws Exception {
            if (lockStatus == LockStatus.UNLOCK) {
                return;
            }

            deleteLockNode(name);
            lockStatus = LockStatus.UNLOCK;
            lockBarrier.reset();
            System.out.println(name + " 释放锁");
            name = null;
        }

        @Override
        public void process(WatchedEvent event) {
            if(Event.KeeperState.SyncConnected == event.getState()){
                return;
            }
            if(Event.EventType.None == event.getType() && event.getPath() == null){
                connectedSemaphore.countDown();
            }
            else if(Event.EventType.NodeChildrenChanged == event.getType() && event.getPath().equals(LOCK_NODE_PARENT_PATH)){
                if(lockStatus != LockStatus.TRY_LOCK){
                    return;
                }
                List<String> nodes = null;
                try {
                    nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
                    nodes.sort(nameComparator);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
                // 判断前面是否有写操作
                if (isFirstNode(name, nodes)) {
                    lockStatus = LockStatus.LOCKED;
                    try {
                        lockBarrier.await();
                        System.out.println(name + " 获取锁");
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        //创建节点
        private String createLockNode(String name){
            String path = null;
            try {
                path = zooKeeper.create(LOCK_NODE_PARENT_PATH + "/" + name, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException | InterruptedException e) {
                System.out.println(" failed to create lock node");
                return null;
            }
            return path;
        }

        //删除节点
        private void deleteLockNode(String name) throws Exception{
            Stat stat = zooKeeper.exists(LOCK_NODE_PARENT_PATH + "/" + name , false);
            zooKeeper.delete(LOCK_NODE_PARENT_PATH + "/" + name , stat.getVersion());
        }

        private String getPrefix(String name) {
            return name.substring(0, name.lastIndexOf('-') + 1);
        }

        private Integer getSequence(String name) {
            return Integer.valueOf(name.substring(name.lastIndexOf("-") + 1));
        }


        private Boolean isFirstNode(String name, List<String> nodes) {
            return nodes.get(0).equals(name);
        }

        private Boolean canAcquireLock(String name , List<String> nodes){
            if(isFirstNode(name , nodes)){
                return true;
            }
            Map<String,Boolean> map = new HashMap<>();
            boolean hasWriteoperation = false;
            for(String n : nodes){
                if(n.contains("read") && !hasWriteoperation){
                    map.put(n, true);
                }else{
                    hasWriteoperation = true;
                    map.put((n), false);
                }
            }
            return map.get(name);
        }
    }


    public static void main(String[] args){
        System.out.println(new Random(System.nanoTime()).nextInt(10000000) + "-read-");
    }

}
