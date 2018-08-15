package com.xzy.zookeeper.lock;/**
 * Created by Administrator on 2018-08-04.
 */

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xuzhiyong
 * @createDate 2018-08-04-13:13
 */
public class ExclusiveLockTest {

    @Test
    public void lock() throws Exception{
        Task task = new Task();
        task.run();
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
            executorService.submit(task);
        }
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    class Task implements Runnable{

        @Override
        public void run() {
            try {
                ExclusiveLock exclusiveLock = new ExclusiveLock();
                exclusiveLock.lock();
                System.out.println("xxx");
                Thread.sleep(2000);
                exclusiveLock.unlock();
            } catch (Exception e){

            }
        }
    }
}
