package com.gmail.eksuzyan.pavel.concurrency.functionality;

import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl.HealthySlave;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 11.04.2017.
 */
@SuppressWarnings("ConstantConditions")
public class InitSlaveTest {

    @Test
    public void passCorrectName() {
        String name = "Semen";
        Slave slave = new HealthySlave(name);
        Assert.assertEquals(name, slave.getName());
    }

    @Test
    public void passNameAsNull() {
        String name = null;
        Slave slave = new HealthySlave(name);
        Assert.assertNotNull(slave.getName());
    }

    @Test
    public void passNameAsEmpty() {
        String name = "";
        Slave slave = new HealthySlave(name);
        Assert.assertNotEquals(name, slave.getName());
    }

}
