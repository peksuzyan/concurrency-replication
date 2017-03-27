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

    private final static Duration DELAY = Duration.of(60, ChronoUnit.SECONDS);

    public static void main(String[] args) throws InterruptedException {

        Thread.currentThread().setName("mainThread");

        Master master = new Master(
                new Slave(),
                new ThrowingSlave(),
                new PendingSlave());

        master.postProject("England", "London");
        master.postProject("Germany", "Berlin");
        master.postProject("Russia", "Moscow");
        master.postProject("USA", "Washington");
        master.postProject("Italy", "Rome");
        master.postProject("Russia", "Saint-Petersburg");
        master.postProject("England", "Manchester");
        master.postProject("Scotland", "Glasgow");
        master.postProject("Russia", "Krasnodar");
        master.postProject("Italy", "Milan");
        master.postProject("France", "Paris");
        master.postProject("Finland", "Helsinki");
        master.postProject("Spain", "Madrid");
        master.postProject("France", "Marcel");
        master.postProject("Italy", "Verona");
        master.postProject("Russia", "Vladivostok");
        master.postProject("Spain", "Barcelona");

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
