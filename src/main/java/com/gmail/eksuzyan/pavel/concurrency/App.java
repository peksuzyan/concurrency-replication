package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.HealthySlave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.PendingSlave;
import com.gmail.eksuzyan.pavel.concurrency.slave.impl.ThrowingSlave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 15.03.2017.
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private final static Duration DELAY = Duration.of(5, ChronoUnit.SECONDS);

    public static void main(String[] args) throws InterruptedException {

        Thread.currentThread().setName("mainThread");

        HealthyMaster master = new HealthyMaster(
                new HealthySlave("healthy-1"),
                new HealthySlave("healthy-3"),
                new PendingSlave("sleepy"),
                new HealthySlave("healthy-2"),
                new ThrowingSlave("throwiny")
        );

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

        System.out.println("======================= " + master.getName().toUpperCase() + " =======================");
        System.out.println("Projects:  " + System.lineSeparator() + printProjects(master.getProjects()));
        System.out.println("Failed:    " + System.lineSeparator() + printRequests(master.getFailed()));

        master.getSlaves().forEach(slave -> {
            System.out.println("======================= " + slave.getName().toUpperCase() + " =======================");
            System.out.println("Projects:  " + System.lineSeparator() + printProjects(slave.getProjects()));
            System.out.println("Deeply equal: " +
                    String.valueOf(Objects.deepEquals(master.getProjects(), slave.getProjects())).toUpperCase());
            try {
                slave.close();
            } catch (IOException e) {
                LOG.error("Main thread has exploded unexpectedly due to IOException!");
            }
        });

        System.out.println("======================================================");
    }

    private static String printProjects(Collection<Project> projects) {
        return projects.stream().map(Project::toString).collect(Collectors.joining(System.lineSeparator()));
    }

    private static String printRequests(Collection<Request> projects) {
        return projects.stream().map(Request::toString).collect(Collectors.joining(System.lineSeparator()));
    }
}
