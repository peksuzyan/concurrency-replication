package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.stores.Master;
import com.gmail.eksuzyan.pavel.concurrency.stores.Slave;
import com.gmail.eksuzyan.pavel.concurrency.stores.ThrowingSlave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author Pavel Eksuzian.
 *         Created: 15.03.2017.
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private final static Duration DELAY = Duration.of(15, ChronoUnit.SECONDS);

    public static void main(String[] args) {

        Slave[] slaves = {new Slave(), new ThrowingSlave()};

        try (Master master = new Master(slaves)) {

            master.postProject("a4sf56", "hello, world!");
            master.postProject("a4sf56", "can't stop!");
            master.postProject("5asda21", "woooh!");
            master.postProject("a4sf56", "ptptptptp");
            master.postProject("ujyhyn3", "lallallalal");

            Thread.sleep(DELAY.toMillis());

            System.out.println("======================= MASTER =======================");
            System.out.println("Projects:  " + master.getProjects().toString());
            System.out.println("Failed:    " + master.getFailed().toString());

            Arrays.asList(slaves).forEach(slave -> {
                System.out.println("======================= SLAVE #" + slave.id + " =======================");
                System.out.println("Projects:  " + slave.getProjects().toString());
            });

            System.out.println("======================================================");

        } catch (Throwable e) {
            LOG.error("Unhandled Exception occurred.", e);
        }
    }

}
