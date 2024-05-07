/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.example;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3WriterPerf {
    private static final Logger LOG = LoggerFactory.getLogger(S3WriterPerf.class);

    // write perf test
    //
    public static void main(String[] args) throws Exception {

        // parse the args bucket, dir, data-size, data-block-per-file, file-count from the args
        // if the args is not provided, use the default value

        final String bucket = getArgs(args, "--bucket", "");
        final String dir = getArgs(args, "--dir", "");
        final int dataSize = Integer.parseInt(getArgs(args, "--data-size", "1024"));
        final int dataBlockPerFile = Integer.parseInt(getArgs(args, "--data-block-per-file", "1024"));
        final int fileCount = Integer.parseInt(getArgs(args, "--file-count", "10"));
        final String region = getArgs(args, "--region", "");
        final int parallel = Integer.parseInt(getArgs(args, "--parallel", "1"));

        ExecutorService service = Executors.newFixedThreadPool(parallel);

        System.out.println("================================");
        System.out.println("Arguments:");
        System.out.println("bucket: " + bucket);
        System.out.println("dir: " + dir);
        System.out.println("dataSize: " + dataSize);
        System.out.println("dataBlockPerFile: " + dataBlockPerFile);
        System.out.println("fileCount: " + fileCount);
        System.out.println("region: " + region);
        System.out.println("parallel: " + parallel);
        System.out.println("Testing time: " + new Date());
        System.out.println("================================");

        S3Client s3 = S3Client.builder().region(Region.of(region)).build();
        // create a single buffer for the file content
        byte[] data = generateData(dataSize * dataBlockPerFile);

        final String filename =  dir + "/" + "test-" + dataSize + "-" + dataBlockPerFile + "-" + fileCount + "-";

        CountDownLatch latch = new CountDownLatch(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int finalI = i;
            service.execute(() -> {
                try {
                    long start = System.nanoTime();
                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(filename + finalI)
                            .build();
                    s3.putObject(putObjectRequest, RequestBody.fromBytes(data));
                    long done = System.nanoTime();
                    System.out.println("File write cost " + TimeUnit.NANOSECONDS.toMillis(done - start) + " ms");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println("Completed at " + new Date());
        s3.close();
        System.exit(0);
    }

    public static String getArgs(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    public static byte[] generateData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 128);
        }
        return data;
    }
}
