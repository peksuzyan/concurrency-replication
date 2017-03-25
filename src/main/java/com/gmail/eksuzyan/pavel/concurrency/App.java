package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.stores.Master;
import com.gmail.eksuzyan.pavel.concurrency.stores.PendingSlave;
import com.gmail.eksuzyan.pavel.concurrency.stores.Slave;
import com.gmail.eksuzyan.pavel.concurrency.stores.ThrowingSlave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * @author Pavel Eksuzian.
 *         Created: 15.03.2017.
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private final static Duration DELAY = Duration.of(5, ChronoUnit.SECONDS);

    public static void main(String[] args) throws InterruptedException {

        Thread.currentThread().setName("mainThread");

        Master master = new Master(
                new Slave(),
                new ThrowingSlave(),
                new PendingSlave());

        master.postProject("a4sf56", "hello, world!");
        master.postProject("a4sf56", "can't stop!");
        master.postProject("5asda21", "woooh!");
        master.postProject("a4sf56", "ptptptptp");
        master.postProject("ujyhyn3", "lallallalal");

        Thread.sleep(DELAY.toMillis());

        master.close();

        System.out.println("======================= MASTER =======================");
        System.out.println("Projects:  \r\n" + master.getProjects().toString());
        System.out.println("Failed:    \r\n" + master.getFailed().toString());

        master.slaves.values().forEach(slave -> {
            System.out.println("======================= SLAVE #" + slave.id + " =======================");
            System.out.println("Projects:  \r\n" + slave.getProjects().toString());
        });

        System.out.println("======================================================");
    }

}
