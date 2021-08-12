package io.smallrye.reactive.messaging.kafka.base;

import java.util.Random;

public class PerfTestUtils {

    private PerfTestUtils() {
        // Avoid direct instantiation
    }

    private static volatile long consumedCPU = System.nanoTime();

    private static final Random RANDOM = new Random();

    // Copied from BlackHole.consumeCPU
    public static void consumeCPU(long tokens) {
        long t = consumedCPU;
        for (long i = tokens; i > 0; i--) {
            t += (t * 0x5DEECE66DL + 0xBL + i) & (0xFFFFFFFFFFFFL);
        }
        if (t == 42) {
            consumedCPU += t;
        }
    }

    public static byte[] generateRandomPayload(int size) {
        byte[] ba = new byte[size];
        RANDOM.nextBytes(ba);
        return ba;
    }
}
