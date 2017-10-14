package com.gmail.eksuzyan.pavel.concurrency.functionality;

import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.AlwaysThrowingSlave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.HealthySlave;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 13.05.2017.
 */
@SuppressWarnings("Duplicates")
public class GeneralMasterTest {

    private static final int DELAY = 100;

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
    public void postAndGetProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(projectsCount, master.getProjects().size());
    }

    @Test
    public void postAndGetFailed() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(0, master.getFailed().size());
    }

    @Test
    public void postTwoSameAndGetProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 2;

        master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project", "data_" + i);

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void postWithNullIdAndGetProjects() {
        Master master = new HealthyMaster();
        master.postProject(null, "city");
    }

    @Test
    public void postWithNullDataAndGetProjects() throws InterruptedException {

        final int projectsCount = 1;

        master = new HealthyMaster();

        master.postProject("project", null);

        Thread.sleep(DELAY);

        Assert.assertEquals(projectsCount, master.getProjects().size());
    }

    @Test
    public void postProjectAndGetNotEmptyFailed() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(new AlwaysThrowingSlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            try {
                master.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        Assert.assertEquals(projectsCount, master.getFailed().size());
    }

    @Test
    public void postProjectAndGetEmptyFailed() throws InterruptedException, IOException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(new HealthySlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        master.close();

        latch.await();

        Assert.assertEquals(0, master.getFailed().size());
    }

    @Test
    public void getAndModifyProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            latch.countDown();
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        master.getProjects().add(new Project("project", "data"));

        Assert.assertEquals(projectsCount, master.getProjects().size());
    }

    @Test
    public void getAndModifyFailed() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        master = new HealthyMaster(new AlwaysThrowingSlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            try {
                master.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        Thread.sleep(DELAY);

        latch.await();

        master.getFailed().add(new Request(null, null));

        Assert.assertEquals(projectsCount, master.getFailed().size());
    }

    @Test
    public void getAndModifySlaves() throws InterruptedException {

        Slave[] slaves = new Slave[]{new HealthySlave()};

        master = new HealthyMaster(slaves);

        master.getSlaves().add(new HealthySlave());

        Assert.assertEquals(slaves.length, master.getSlaves().size());
    }

    @Test(expected = IllegalStateException.class)
    public void closeMasterAndPostProject() throws InterruptedException, IOException {

        master = new HealthyMaster(null);

        master.close();

        master.postProject("project", "data");
    }

    @Test(expected = IllegalStateException.class)
    public void closeMasterAndCloseAgain() throws InterruptedException, IOException {

        master = new HealthyMaster(null);

        master.close();

        master.close();
    }

}
