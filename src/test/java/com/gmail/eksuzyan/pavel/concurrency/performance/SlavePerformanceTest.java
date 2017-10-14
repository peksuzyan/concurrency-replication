package com.gmail.eksuzyan.pavel.concurrency.performance;

import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.HealthySlave;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.04.2017.
 */
@SuppressWarnings("Duplicates")
public class SlavePerformanceTest {

    private Master master;

    @After
    public void tearDown() throws IOException {
        try {
            if (master != null) master.close();
            MasterProperties.reset();
        } catch (IllegalStateException ignore) {
            /* NOP */
        }
    }

    @Test
    public void postProject() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project", "data");

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(10);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postDecadeProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 10;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(25);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postHundredProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 100;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(50);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postThousandProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1_000;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(100);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postDecadeThousandsProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 10_000;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(1_000);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postHundredThousandsProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 100_000;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(15_000);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postMillionProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1_000_000;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(15_000);

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postProjectsAndCheckFailed() throws InterruptedException, IOException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 100_000;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(15_000);

        master.close();

        int expected = 10;
        boolean result = master.getFailed().size() < expected;

        Assert.assertTrue(result);
    }

}
