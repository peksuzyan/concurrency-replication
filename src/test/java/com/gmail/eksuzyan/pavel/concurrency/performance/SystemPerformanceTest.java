package com.gmail.eksuzyan.pavel.concurrency.performance;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
@Ignore
@SuppressWarnings({"ResultOfMethodCallIgnored", "MismatchedQueryAndUpdateOfCollection"})
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

    @Test
    public void testLocalDateTimeGeneration() {

        long start = System.currentTimeMillis();

        LocalDateTime current = LocalDateTime.now();

        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start)); // ~ 80-100 ms
    }

    @Test
    public void testRequestGeneration() {

        long start = System.currentTimeMillis();

        Request request = new Request(null, new Project("id", "data"));

        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start)); // ~ 90-110 ms
    }

    @Test
    public void testObjectGeneration() {

        long start = System.currentTimeMillis();

        Object current = new Object();

        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start));
    }

    @Test
    public void testDateGeneration() {

        long start = System.currentTimeMillis();

        Date current = new Date();

        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start));
    }
}
