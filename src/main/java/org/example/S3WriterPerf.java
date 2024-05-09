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

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class S3WriterPerf {
    private static final Logger LOG = LoggerFactory.getLogger(S3WriterPerf.class);

    // write perf test
    //
    public static void main(String[] args) throws Exception {
        new S3WriterPerf().run(args);
    }

    List<AutoCloseable> closeables = Collections.synchronizedList(new ArrayList<>());
    Map<Long, S3AsyncClient> s3AsyncClients = new ConcurrentHashMap<>();
    Region region;

    public void run(String[] args) throws Exception {

        // parse the args bucket, dir, data-size, data-block-per-file, file-count from the args
        // if the args is not provided, use the default value

        final String bucket = getArgs(args, "--bucket", "");
        final String dir = getArgs(args, "--dir", "");
        final int dataSize = Integer.parseInt(getArgs(args, "--data-size", "1024"));
        final int dataBlockPerFile = Integer.parseInt(getArgs(args, "--data-block-per-file", "1024"));
        final int fileCount = Integer.parseInt(getArgs(args, "--file-count", "10"));
        final String region = getArgs(args, "--region", "");
        final int parallel = Integer.parseInt(getArgs(args, "--parallel", "1"));

        this.region = Region.of(region);
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

        // create a single buffer for the file content
        byte[] data = generateData(dataSize * dataBlockPerFile);
        // pre-calculate the CRC32C checksum for the data so that we don't spend CPU on this later
        String crc32cBase64 = calculateCRC32CBase64Encoded(data);

        final String filename =  dir + "/" + "test-" + dataSize + "-" + dataBlockPerFile + "-" + fileCount + "-";

        List<Long> allLatencies = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int finalI = i;
            service.execute(() -> {
                try {
                    S3AsyncClient s3AsyncClient = getS3AsyncClient();
                    long start = System.nanoTime();
                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(filename + finalI)
                            .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
                            .checksumCRC32C(crc32cBase64)
                            .build();
                    PutObjectResponse putObjectResponse =
                            s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromBytes(data)).get();
                    long done = System.nanoTime();
                    long latency = TimeUnit.NANOSECONDS.toMillis(done - start);
                    System.out.println("File write cost " + latency + " ms");
                    allLatencies.add(latency);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.println("Completed at " + new Date());

        closeables.forEach(c -> {
            try {
                c.close();
            } catch (Exception e) {
                // ignore
            }
        });

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

    private static String calculateCRC32CBase64Encoded(byte[] data) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(data);
        int crc32cValue = (int)crc32c.getValue();
        String crc32cBase64 = Base64.getEncoder().encodeToString(new byte[] {
            (byte) (crc32cValue >>> 24),
            (byte) (crc32cValue >>> 16),
            (byte) (crc32cValue >>> 8),
            (byte) crc32cValue
        });
        return crc32cBase64;
    }

    private S3AsyncClient getS3AsyncClient() {
        return s3AsyncClients.computeIfAbsent(Thread.currentThread().getId(), id -> {
            SdkAsyncHttpClient nettyHttpClient = NettyNioAsyncHttpClient.builder()
                    .useNonBlockingDnsResolver(true)
                    .maxConcurrency(100).build();
            S3AsyncClient s3AsyncClient =
                    S3AsyncClient.builder()
                            .httpClient(nettyHttpClient)
                            .region(region)
                            // CreateSession authentication is enabled by default, just make it explicit here
                            .disableS3ExpressSessionAuth(false)
                            .build();
            closeables.add(s3AsyncClient);
            closeables.add(nettyHttpClient);
            return s3AsyncClient;
        });
    }

    private static void showPercentiles(List<Long> input) {
        List<Long> latencies = new ArrayList<>(input);
        latencies.sort(Long::compareTo);
        System.out.println("50th percentile (Median): " + calculatePercentile(latencies, 50.0) + " ms");
        System.out.println("75th percentile: " + calculatePercentile(latencies, 75.0) + " ms");
        System.out.println("90th percentile: " + calculatePercentile(latencies, 90.0) + " ms");
        System.out.println("95th percentile: " + calculatePercentile(latencies, 95.0) + " ms");
        System.out.println("99th percentile: " + calculatePercentile(latencies, 99.0) + " ms");
        System.out.println("Mean: " + (long) latencies.stream().mapToLong(Long::longValue).average().orElse(0) + " ms");
        System.out.println("Min: " + latencies.get(0) + " ms");
        System.out.println("Max: " + latencies.get(latencies.size() - 1) + " ms");
    }

    private static long calculatePercentile(List<Long> sortedLatencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size());
        return sortedLatencies.get(index - 1);
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
