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

import java.net.URI;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

        final FileSystem fs = FileSystem.get(URI.create(bucket), configuration);
        byte[] data = generateData(dataSize);

        final String filename =  "/" + dir + "/" + "test-" + dataSize + "-" + dataBlockPerFile + "-" + fileCount + "-";

        CountDownLatch latch = new CountDownLatch(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int finalI = i;
            service.execute(() -> {
                try {
                    long start = System.currentTimeMillis();
                    FSDataOutputStream output = fs.create(new Path(filename + finalI));
                    long createdFileTime = System.currentTimeMillis();
                    for (int j = 0; j < dataBlockPerFile; j++) {
                        output.write(data);
                    }
                    output.close();
                    long done = System.currentTimeMillis();
                    System.out.println("File created cost " + (createdFileTime - start) + " ms" + ", write cost " + (done - createdFileTime) + " ms");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
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
