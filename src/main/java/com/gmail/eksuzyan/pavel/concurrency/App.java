package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.controllers.MasterController;
import com.gmail.eksuzyan.pavel.concurrency.controllers.SlaveController;
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

    private final static Duration DELAY = Duration.of(3, ChronoUnit.SECONDS);

    public static void main(String[] args) {

        SlaveController[] slaves = {new SlaveController(), new SlaveController()};

        try (MasterController master = new MasterController(slaves)) {
            master.postProject("a4sf56", "hello, world!");

            Thread.sleep(DELAY.toMillis());

            System.out.println(master.getFailed());
            System.out.println(master.getWaiting());
        } catch (Throwable e) {
            LOG.error("Unhandled Exception occurred.", e);
        }
    }

}
