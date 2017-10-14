package com.gmail.eksuzyan.pavel.concurrency.performance;

import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.impl.HealthyMaster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Pavel Eksuzian.
 *         Created: 11.04.2017.
 */
@SuppressWarnings("Duplicates")
public class MasterPerformanceTest {

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

        master = new HealthyMaster();

        master.postProject("country", "city");

        Thread.sleep(4);

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test
    public void postProjects() throws InterruptedException {

        master = new HealthyMaster();

        master.postProject("country_1", "city");
        master.postProject("country_2", "city");

        Thread.sleep(4);

        Assert.assertEquals(2, master.getProjects().size());
    }

    @Test
    public void postProjectsWithTheSameIds() throws InterruptedException {

        master = new HealthyMaster();

        master.postProject("country", "city_1");
        master.postProject("country", "city_2");

        Thread.sleep(4);

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test
    public void postManyProjects() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1_000_000;

        master = new HealthyMaster();

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project_" + i, "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(150);

        Assert.assertEquals(projectsCount, master.getProjects().size());
    }

    @Test
    public void postManyProjectsWithTheSameIds() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final int projectsCount = 1_000_000;

        master = new HealthyMaster();

        new Thread(() -> {
            int i = 0;
            while (i++ < projectsCount)
                master.postProject("project", "data_" + i);

            latch.countDown();
        }).start();

        latch.await();

        Thread.sleep(150);

        Assert.assertEquals(1, master.getProjects().size());
    }

}
