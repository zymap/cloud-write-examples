package org.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            executorService.execute(() -> {
                System.out.println("Hello, World!");
                try {

                    TimeUnit.SECONDS.sleep(10);
                } catch (Exception e) {
                    // ignore
                }
                latch.countDown();
            });
        }

        latch.await();
    }
}
