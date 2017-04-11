package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 11.04.2017.
 */
@SuppressWarnings("Duplicates")
public class MasterPerformanceTest {

    @Test
    public void postProject() {

        Master master = new HealthyMaster();

        master.postProject("country", "city");

        try {
            Thread.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void postProjectWithNullId() {
        Master master = new HealthyMaster();
        master.postProject(null, "city");
    }

    @Test
    public void postProjectWithNullData() {
        Master master = new HealthyMaster();

        master.postProject("country", null);

        try {
            Thread.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test
    public void postProjects() {
        Master master = new HealthyMaster();

        master.postProject("country_1", "city");
        master.postProject("country_2", "city");

        try {
            Thread.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(2, master.getProjects().size());
    }

    @Test
    public void postProjectsWithTheSameIds() {
        Master master = new HealthyMaster();

        master.postProject("country", "city_1");
        master.postProject("country", "city_2");

        try {
            Thread.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, master.getProjects().size());
    }

    @Test
    public void postManyProjects() {

        Master master = new HealthyMaster();

        int messages = 1_000_000;

        for (int i = 0; i < messages; i++)
            master.postProject("country_" + i, "city_" + i);

        try {
            Thread.sleep(125);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(messages, master.getProjects().size());
    }

    @Test
    public void postManyProjectsWithTheSameIds() {

        Master master = new HealthyMaster();

        int messages = 1_000_000;

        for (int i = 0; i < messages; i++)
            master.postProject("country", "city_" + i);

        try {
            Thread.sleep(125);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, master.getProjects().size());
    }

}
