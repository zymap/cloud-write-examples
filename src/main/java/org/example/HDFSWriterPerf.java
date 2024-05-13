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

import static org.example.S3WriterPerf.generateRandomString;
import static org.example.S3WriterPerf.showPercentiles;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSWriterPerf {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSWriterPerf.class);

    // write perf test
    //
    public static void main(String[] args) throws Exception {

//        bin/write-perf --bucket s3a://yong-dev-us-west-2 --dir test-perf-8 --file-count 100 --data-size 1024 --data-block-per-file 512 --parallel 10 --region us-west-2
        // parse the args bucket, dir, data-size, data-block-per-file, file-count from the args
        // if the args is not provided, use the default value

        final String bucket = getArgs(args, "--bucket", "");
        final String dir = getArgs(args, "--dir", "");
        final int dataSize = Integer.parseInt(getArgs(args, "--data-size", "1024"));
        final int dataBlockPerFile = Integer.parseInt(getArgs(args, "--data-block-per-file", "1024"));
        final int fileCount = Integer.parseInt(getArgs(args, "--file-count", "10"));
        final String region = getArgs(args, "--region", "");
        final int parallel = Integer.parseInt(getArgs(args, "--parallel", "1"));

        Configuration configuration = new Configuration();
        configuration.set("fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider");
        if (!region.isEmpty()) {
            configuration.set("fs.s3a.endpoint.region", region);
        }

        String randomSuffix = generateRandomString(8);
        String finalDir = dir + randomSuffix;

        ExecutorService service = Executors.newFixedThreadPool(parallel);

        System.out.println("================================");
        System.out.println("Arguments:");
        System.out.println("bucket: " + bucket);
        System.out.println("dir: " + finalDir);
        System.out.println("dataSize: " + dataSize);
        System.out.println("dataBlockPerFile: " + dataBlockPerFile);
        System.out.println("fileCount: " + fileCount);
        System.out.println("region: " + region);
        System.out.println("parallel: " + parallel);
        System.out.println("Testing time: " + new Date());
        System.out.println("================================");

        final FileSystem fs = FileSystem.get(URI.create(bucket), configuration);
        byte[] data = generateData(dataSize);

        final String filename =  "/" + finalDir + "/" + "test-" + dataSize + "-" + dataBlockPerFile + "-" + fileCount + "-";

        List<Long> allLatencies = new ArrayList<>();

        CountDownLatch latch = new CountDownLatch(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int finalI = i;
            service.execute(() -> {
                try {
                    long start = System.nanoTime();
                    FSDataOutputStream output = fs.create(new Path(filename + finalI));
                    long createdFileTime = System.currentTimeMillis();
                    for (int j = 0; j < dataBlockPerFile; j++) {
                        output.write(data);
                    }
                    output.close();
                    long done = System.nanoTime();
                    long latency = TimeUnit.NANOSECONDS.toMillis(done - start);
                    allLatencies.add(latency);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println("Completed at " + new Date());
        fs.close();


        if (!allLatencies.isEmpty()) {
            System.out.println("================================");
            System.out.println("Latency percentiles for all requests:");
            showPercentiles(allLatencies);
            System.out.println("================================");
            System.out.println("Latency percentiles for last 50% of requests:");
            showPercentiles(allLatencies.subList(allLatencies.size() / 2, allLatencies.size()));
            System.out.println("================================");
        }
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
