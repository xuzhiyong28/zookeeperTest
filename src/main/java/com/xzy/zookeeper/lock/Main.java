package com.xzy.zookeeper.lock;

import java.io.IOException;

/**
 * @Auther: xuzy
 * @Date: 2018/8/3 18:14
 * @Description:
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        LockStatus  lockStatus = LockStatus.UNLOCK;
        System.out.println(lockStatus);
    }
}
