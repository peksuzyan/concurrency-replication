package com.gmail.eksuzyan.pavel.concurrency;

import org.junit.Test;

import java.util.PriorityQueue;
import java.util.Queue;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public class SystemPerformanceTest {

    @Test(timeout = 5000)
    public void testMessagesWithoutKeeping() {

        for (int i = 0; i < 100_000_000; i++) {
            String.valueOf(i);
        }
    }

    @Test(timeout = 5000)
    public void testMessagesKeepingIntoArray() {

        String[] array = new String[5_000_000];

        for (int i = 0; i < array.length; i++) {
            array[i] = String.valueOf(i);
        }
    }

    @Test(timeout = 5000)
    public void testMessagesOutput() {

        for (int i = 0; i < 950_000; i++) {
            System.out.println(String.valueOf(i));
        }
    }

    @Test(timeout = 5000)
    public void testMessagesOutputWithConcat() {

        for (int i = 0; i < 900_000; i++) {
            System.out.println("Project v." + String.valueOf(i));
        }
    }

    @Test
    public void testPostDuration() {

        Queue<String> queue = new PriorityQueue<>();

        int messages = 1_000_000;

        long start = System.currentTimeMillis();

        for (int i = 0; i < messages; i++)
            queue.add("country_" + i);

        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start));
    }

}
