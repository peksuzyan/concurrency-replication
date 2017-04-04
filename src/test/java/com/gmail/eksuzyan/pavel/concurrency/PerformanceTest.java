package com.gmail.eksuzyan.pavel.concurrency;

import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public class PerformanceTest {

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

}
