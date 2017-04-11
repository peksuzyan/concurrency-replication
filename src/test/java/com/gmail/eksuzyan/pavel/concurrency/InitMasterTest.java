package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 11.04.2017.
 */
public class InitMasterTest {

    @Test
    public void passCorrectName() {
        String name = "Igor";
        Master master = new HealthyMaster(name);
        Assert.assertEquals(name, master.getName());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void passNameAsNull() {
        String name = null;
        Master master = new HealthyMaster(name);
        Assert.assertNotNull(master.getName());
    }

    @Test
    public void passNameAsEmpty() {
        String name = "";
        Master master = new HealthyMaster(name);
        Assert.assertNotEquals(name, master.getName());
    }

    @Test
    public void passNoSlaves() {
        Master master = new HealthyMaster();
        Assert.assertEquals(0, master.getSlaves().size());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void passSlavesAsNull() {
        Slave[] slaves = null;
        Master master = new HealthyMaster(slaves);
        Assert.assertEquals(0, master.getSlaves().size());
    }

    @SuppressWarnings("RedundantArrayCreation")
    @Test
    public void passSlavesAsEmptyArray() {
        Master master = new HealthyMaster(new Slave[0]);
        Assert.assertEquals(0, master.getSlaves().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void passSlavesWithNull() {
        Slave[] slaves = new Slave[] {null};
        Master master = new HealthyMaster(slaves);
    }

    @Test
    public void passSingleSlave() {
        Master master = new HealthyMaster(new HealthySlave());
        Assert.assertEquals(1, master.getSlaves().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void passTheSameSlaves() {
        Slave slave = new HealthySlave();
        Master master = new HealthyMaster(slave, slave);
    }

    @Test
    public void passManySlaves() {

        Slave[] slaves = new Slave[30];
        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new HealthySlave("my_slave_" + (i + 1));
        }

        Master master = new HealthyMaster(slaves);

        Assert.assertEquals(slaves.length, master.getSlaves().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void passSlavesWithTheSameNames() {

        Slave[] slaves = new Slave[30];
        for (int i = 0; i < slaves.length; i++) {
            slaves[i] = new HealthySlave("my_slave_" + (i + 1));
        }

        slaves[slaves.length / 2] = new HealthySlave("my_slave_1");

        Master master = new HealthyMaster(slaves);
    }

}
