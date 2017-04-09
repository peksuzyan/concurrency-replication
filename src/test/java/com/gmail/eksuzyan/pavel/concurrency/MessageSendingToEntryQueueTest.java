package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.PendingSlave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.ThrowingSlave;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    }

    @Test
    public void testManyThreadsWithManyMessagesUniqueProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = messagesPerThread * counter; j < messagesPerThread * (counter + 1); j++) {
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

        System.out.println(master.getName() + ": " + master.getProjects().size() + " project(s).");
        master.getSlaves().forEach(s ->
                System.out.println(s.getName() + ": " + s.getProjects().size() + " project(s)."));
        System.out.println(master.getName() + ": " + master.getFailed().size() + " request(s).");
    }

    @Test
    public void testManyThreadsWithManyMessagesEqualProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
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
    public void testManyThreadsWithManyMessagesUniqueProjectsManyHealthySlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new HealthySlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = messagesPerThread * counter; j < messagesPerThread * (counter + 1); j++) {
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
    public void testManyThreadsWithManyMessagesUniqueProjectsManyPendingSlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new PendingSlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = messagesPerThread * counter; j < messagesPerThread * (counter + 1); j++) {
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
    public void testManyThreadsWithManyMessagesUniqueProjectsManyThrowingSlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new ThrowingSlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = messagesPerThread * counter; j < messagesPerThread * (counter + 1); j++) {
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
    public void testManyThreadsWithManyMessagesUniqueProjectsManyDifferentSlaves()
            throws InterruptedException {

        List<Slave> slaves = new ArrayList<>();

        for (int i = 0; i < 1_000_000 / 3; i++) {
            slaves.add(new HealthySlave());
            slaves.add(new PendingSlave());
            slaves.add(new ThrowingSlave());
        }

        Master master = new HealthyMaster(slaves.toArray(new Slave[slaves.size()]));

        final int messagesPerThread = 10;
        final int threadsCount = 6_000;

        final CountDownLatch startLatch = new CountDownLatch(threadsCount);
        final CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            final int counter = i;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = messagesPerThread * counter; j < messagesPerThread * (counter + 1); j++) {
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
