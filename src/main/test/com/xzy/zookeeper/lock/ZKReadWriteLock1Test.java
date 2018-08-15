package com.xzy.zookeeper.lock;/**
 * Created by Administrator on 2018-08-05.
 */

import org.junit.Test;

/**
 * @author xuzhiyong
 * @createDate 2018-08-05-8:00
 */
public class ZKReadWriteLock1Test {
    @Test
    public void test() throws Exception{
        ZKReadWriteLock1 zkReadWriteLock1 = new ZKReadWriteLock1();
        zkReadWriteLock1.readLock().lock();
        Thread.sleep(2000);
        zkReadWriteLock1.readLock().unlock();
    }
}
