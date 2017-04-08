package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public class MessageSendingToEntryQueueTest {

    @Test(timeout = 5000)
    public void testOneThreadWithManyMessagesEqualProjects() {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 1_000_000;

        for (int i = 0; i < threadsCount; i++) {
            master.postProject("Michael", "Jackson_" + String.valueOf(i));
        }
    }

    @Test(timeout = 5000)
    public void testOneThreadWithManyMessagesUniqueProjects() {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 1_000_000;

        for (int i = 0; i < threadsCount; i++) {
            master.postProject("Michael_" + String.valueOf(i), "Jackson");
        }
    }

    @Test(timeout = 5000)
    public void testManyThreadsWithOneMessageEqualProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    master.postProject("Michael", "Jackson_" + String.valueOf(counter));
                    finishLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            startLatch.countDown();
        }

        finishLatch.await();

//        System.out.println(master.getName() + ": " + master.getProjects().size() + " project(s).");
//        master.getSlaves().forEach(s ->
//                System.out.println(s.getName() + ": " + s.getProjects().size() + " project(s)."));
    }

    @Test(timeout = 5000)
    public void testManyThreadsWithOneMessageUniqueProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    master.postProject("Michael_" + String.valueOf(counter), "Jackson");
                    finishLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            startLatch.countDown();
        }

        finishLatch.await();

//        System.out.println(master.getName() + ": " + master.getProjects().size() + " project(s).");
//        master.getSlaves().forEach(s ->
//                System.out.println(s.getName() + ": " + s.getProjects().size() + " project(s)."));
    }

    @Test
    public void testManyThreadsWithManyMessagesUniqueProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messages = 1_000_000;
        final int threadsCount = messages / 1_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = threadsCount * counter; j < threadsCount * (counter + 1); j++) {
                        master.postProject("Michael_" + String.valueOf(j), "Jackson");
                    }
                    finishLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            startLatch.countDown();
        }

        finishLatch.await();

    }

    @Test
    public void testManyThreadsWithManyMessagesEqualProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messages = 1_000_000;
        final int threadsCount = messages / 1_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messages / threadsCount; j++) {
                        master.postProject("Michael_" + String.valueOf(j), "Jackson");
                    }
                    finishLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            startLatch.countDown();
        }

        finishLatch.await();

    }

}
