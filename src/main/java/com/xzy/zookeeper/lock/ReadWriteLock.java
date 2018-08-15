package com.xzy.zookeeper.lock;/**
 * Created by Administrator on 2018-08-04.
 */

/**
 * @author xuzhiyong
 * @createDate 2018-08-04-16:38
 */
public interface ReadWriteLock {
    DistributedLock readLock();
    DistributedLock writeLock();

}
