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

        Slave[] slaves = new Slave[]{new HealthySlave()};
        Master master = new HealthyMaster(slaves);

        master.postProject("country", "city");

        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(1, slaves[0].getProjects().size());
    }

    @Test
    public void postProjects() {

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

        Assert.assertEquals(messages, slaves[0].getProjects().size()); // ~700
    }

}
