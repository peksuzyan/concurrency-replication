package com.gmail.eksuzyan.pavel.concurrency.functionality;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.ThrowingSlave;
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

    @Test
    public void postAndGetProjects() throws InterruptedException {

        final int projectsCount = 10_000;

        final Master master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");
        }).start();

        Thread.sleep(10);

        for (Project project : master.getProjects())
            Thread.sleep(1);
    }

    @Test
    public void postProjectAndGetFailed() throws InterruptedException {

        final int projectsCount = 1_000_000;

        final Master master = new HealthyMaster(new ThrowingSlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");
        }).start();

        Thread.sleep(10);

        for (Request request : master.getFailed())
            Thread.sleep(1);
    }

    @Test
    public void getAndModifyProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        final Master master = new HealthyMaster(null);

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        latch.await();

        master.getProjects().add(new Project("project", "data"));

        Assert.assertEquals(projectsCount, master.getProjects().size());
    }

    @Test
    public void getAndModifyFailed() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        final Master master = new HealthyMaster(new ThrowingSlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");

            try {
                Thread.sleep(10);
                master.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();

        latch.await();

        master.getFailed().add(new Request(null, null));

        Assert.assertEquals(projectsCount, master.getFailed().size());
    }

    @Test
    public void getAndModifySlaves() throws InterruptedException {

        Slave[] slaves = new Slave[] {new HealthySlave()};

        Master master = new HealthyMaster(slaves);

        master.getSlaves().add(new HealthySlave());

        Assert.assertEquals(slaves.length, master.getSlaves().size());
    }

}
