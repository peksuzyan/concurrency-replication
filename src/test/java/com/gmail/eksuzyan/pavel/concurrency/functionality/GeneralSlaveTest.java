package com.gmail.eksuzyan.pavel.concurrency.functionality;

import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.DefaultMaster;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.HealthySlave;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 14.05.2017.
 */
@SuppressWarnings("Duplicates")
public class GeneralSlaveTest {

    private static final int DELAY = 100;
    private Master master;

    @After
    public void tearDown() throws IOException {
        if (master != null) master.close();

        MasterProperties.reset();
    }

    @Test
    public void plainTest() throws InterruptedException {

        master = new DefaultMaster(new HealthySlave());

        master.postProject("project", "data");

        Thread.sleep(DELAY);

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test
    public void postAndGetProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new DefaultMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(projectsCount, slaves[0].getProjects().size());
    }

    @Test
    public void postTwoSameAndGetProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 2;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new DefaultMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project", "data_" + i);

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(1, slaves[0].getProjects().size());
    }

    @Test
    public void getAndModifyProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new DefaultMaster(slaves);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        for (Slave slave : slaves) {
            slave.getProjects().add(new Project("project", "data"));
            Assert.assertEquals(projectsCount, slave.getProjects().size());
        }
    }

    @Test
    public void closeSlaveAndPostProject() throws InterruptedException, IOException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new DefaultMaster(slaves);

        for (Slave slave : slaves) {
            slave.close();
        }

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        for (Slave slave : slaves) {
            Assert.assertEquals(0, slave.getProjects().size());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void closeSlaveAndCloseAgain() throws InterruptedException, IOException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new DefaultMaster(slaves);

        for (Slave slave : slaves) {
            slave.close();
        }

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        for (Slave slave : slaves) {
            slave.close();
        }
    }

}
