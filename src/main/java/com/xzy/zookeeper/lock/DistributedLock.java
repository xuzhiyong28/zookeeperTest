package com.xzy.zookeeper.lock;

/**
 * Created by Administrator on 2018-08-04.
 */
public interface DistributedLock {
    void lock() throws Exception;
    Boolean tryLock() throws Exception;
    Boolean tryLock(long millisecond) throws Exception;
    void unlock() throws Exception;
}
