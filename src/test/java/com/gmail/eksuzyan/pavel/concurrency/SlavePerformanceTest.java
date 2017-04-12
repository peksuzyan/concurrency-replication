package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.04.2017.
 */
public class  SlavePerformanceTest {

    @Test
    public void postProject() {

        Slave[] slaves = new Slave[] {new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        master.postProject("country", "city");

        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, slaves[0].getProjects().size());
    }

    @Test
    public void postTwoProjects() {

        Slave[] slaves = new Slave[] {new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        master.postProject("country_1", "city");
        master.postProject("country_2", "city");

        try {
            Thread.sleep(6);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(2, slaves[0].getProjects().size());
    }

    @Test
    public void postProjects() {

        Slave[] slaves = new Slave[] {new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        int messages = 100;

        for (int i = 0; i < messages; i++)
            master.postProject("country_" + i, "city_" + i);

        try {
            Thread.sleep(34);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(messages, slaves[0].getProjects().size());
    }

    @Test
    public void postDecadesProjects() {

        Slave[] slaves = new Slave[]{new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        int messages = 10_000;

        for (int i = 0; i < messages; i++)
            master.postProject("country_" + i, "city_" + i);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(messages, slaves[0].getProjects().size());
        // was: ~700
        // now: ~2400
    }

    @Test
    public void postHundredsProjects() {

        Slave[] slaves = new Slave[]{new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        int messages = 100_000;

        for (int i = 0; i < messages; i++)
            master.postProject("country_" + i, "city_" + i);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(messages, slaves[0].getProjects().size());
    }

    @Test
    public void postThousandsProjects() {

        Slave[] slaves = new Slave[]{new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        int messages = 1_000_000;

        for (int i = 0; i < messages; i++)
            master.postProject("country_" + i, "city_" + i);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(messages, slaves[0].getProjects().size());
    }

}
