package com.gmail.eksuzyan.pavel.concurrency.performance;

import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.HealthySlave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.PendingSlave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.ThrowingSlave;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
@SuppressWarnings("Duplicates")
@Ignore
public class MessageSendingToEntryQueueTest {

    @Test(timeout = 5_000)
    public void testOneThreadWithManyMessagesEqualProjects() {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int projects = 1_000_000;

        for (int i = 0; i < projects; i++) {
            master.postProject("Michael", "Jackson_" + String.valueOf(i));
        }
    }

    @Test(timeout = 5_000)
    public void testOneThreadWithManyMessagesUniqueProjects() {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int projects = 1_000_000;

        for (int i = 0; i < projects; i++) {
            master.postProject("Michael_" + String.valueOf(i), "Jackson");
        }
    }

    @Test(timeout = 10_000)
    public void testManyThreadsWithOneMessageEqualProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 10_000;

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
    }

    @Test(timeout = 10_000)
    public void testManyThreadsWithOneMessageUniqueProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int threadsCount = 10_000;

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

    @Test(timeout = 100_000)
    public void testManyThreadsWithManyMessagesUniqueProjects() throws InterruptedException {

        MasterProperties.setDispatcherThreadPoolSize(128);
        MasterProperties.setPreparatorThreadPoolSize(128);
        MasterProperties.setListenerThreadPoolSize(128);
        MasterProperties.setSchedulerThreadPoolSize(1);
        MasterProperties.setRepeaterThreadPoolSize(1);

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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

    @Test(timeout = 10_000)
    public void testManyThreadsWithManyMessagesEqualProjects() throws InterruptedException {

        Master master = new HealthyMaster(
                new HealthySlave());

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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

    @Test(timeout = 50_000)
    public void testManyThreadsWithManyMessagesUniqueProjectsManyHealthySlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new HealthySlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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

    @Test(timeout = 50_000)
    public void testManyThreadsWithManyMessagesUniqueProjectsManyPendingSlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new PendingSlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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

    @Test(timeout = 50_000)
    public void testManyThreadsWithManyMessagesUniqueProjectsManyThrowingSlaves()
            throws InterruptedException {

        Slave[] slaves = new Slave[1_000];

        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new ThrowingSlave();
        }

        Master master = new HealthyMaster(slaves);

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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

    @Test(timeout = 50_000)
    public void testManyThreadsWithManyMessagesUniqueProjectsManyDifferentSlaves()
            throws InterruptedException {

        List<Slave> slaves = new ArrayList<>();

        for (int i = 0; i < 1_000 / 3; i++) {
            slaves.add(new HealthySlave());
            slaves.add(new PendingSlave());
            slaves.add(new ThrowingSlave());
        }

        Master master = new HealthyMaster(slaves.toArray(new Slave[slaves.size()]));

        final int messagesPerThread = 10;
        final int threadsCount = 10_000;

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
