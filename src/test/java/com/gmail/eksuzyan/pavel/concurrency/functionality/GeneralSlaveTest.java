package com.gmail.eksuzyan.pavel.concurrency.functionality;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
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

    @Test
    public void postAndGetProjects() throws InterruptedException {

        final int projectsCount = 10_000;

        final Master master = new HealthyMaster(new HealthySlave());

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data");
        }).start();

        Thread.sleep(10);

        for (Slave slave : master.getSlaves())
            for (Project project : slave.getProjects())
                Thread.sleep(1);
    }

    @Test
    public void getAndModifyProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1;

        final Master master = new HealthyMaster(new HealthySlave());

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

        master.getSlaves().forEach(slave ->
                slave.getProjects().add(new Project("project", "data")));

        master.getSlaves().forEach(slave ->
                Assert.assertEquals(projectsCount, slave.getProjects().size()));
    }

    @Test
    public void closeMasterAndPostProject() throws InterruptedException, IOException {

        Slave[] slaves = new Slave[] {new HealthySlave()};

        Master master = new HealthyMaster(slaves);

        slaves[0].close();

        master.postProject("project", "data");

        Thread.sleep(50);

        master.getSlaves().forEach(slave ->
                Assert.assertEquals(0, slave.getProjects().size()));
    }

    @Test
    public void closeMasterAndCloseAgain() throws InterruptedException, IOException {

        Slave[] slaves = new Slave[] {new HealthySlave()};

        Master master = new HealthyMaster(slaves);

        slaves[0].close();

        master.postProject("project", "data");

        Thread.sleep(50);

        slaves[0].close();

        master.getSlaves().forEach(slave ->
                Assert.assertEquals(0, slave.getProjects().size()));
    }

}
