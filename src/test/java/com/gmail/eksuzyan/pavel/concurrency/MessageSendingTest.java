package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import org.junit.Test;

/**
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public class MessageSendingTest {

    @Test
    public void testManyMessagesSendingToSingleSlave() {

        Master master = new HealthyMaster(
                new HealthySlave());

        for (int i = 0; i < 1_000_000; i++) {
            master.postProject("England", "London_" + String.valueOf(i));
        }
    }

    @Test
    public void testManyMessagesSendingToTwoSlaves() {

        Master master = new HealthyMaster(
                new HealthySlave(),
                new HealthySlave());

        for (int i = 0; i < 1_000_000; i++) {
            master.postProject("England", "London_" + String.valueOf(i));
        }
    }

    @Test
    public void testManyMessagesSendingToThreeSlaves() {

        Master master = new HealthyMaster(
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave());

        for (int i = 0; i < 1_000_000; i++) {
            master.postProject("England", "London_" + String.valueOf(i));
        }
    }

    @Test
    public void testManyMessagesSendingToManySlaves() {

        Master master = new HealthyMaster(
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave(),
                new HealthySlave());

        for (int i = 0; i < 1_000_000; i++) {
            master.postProject("England", "London_" + String.valueOf(i));
        }
    }
}
