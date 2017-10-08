package com.gmail.eksuzyan.pavel.concurrency;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.master.Master;
import com.gmail.eksuzyan.pavel.concurrency.master.impl.HealthyMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 15.03.2017.
 */
public class App {

    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private final static Duration DELAY = Duration.of(5, ChronoUnit.SECONDS);

    private static Master master;

    private static ExecutorService generator = Executors.newFixedThreadPool(5);

    public static void main(String[] args) throws InterruptedException, IOException {

        Thread.currentThread().setName("mainThread");

        master = new HealthyMaster(
                new HealthySlave("healthy-1"),
                new ThrowingSlave("throwiny-1", 0.3),
                new HealthySlave("healthy-3"),
                new ThrowingSlave("throwiny-3", 0.9),
                new PendingSlave("sleepy"),
                new HealthySlave("healthy-2"),
                new ThrowingSlave("throwiny-2", 0.2)
        );

        deliver("England", "London");
        deliver("Germany", "Berlin");
        deliver("Russia", "Moscow");
        deliver("USA", "Washington");
        deliver("USA", "San-Francisko");
        deliver("Germany", "Essen");
        deliver("Russia", "Ekaterinburg");
        deliver("Italy", "Rome");
        deliver("Spain", "Valencia");
        deliver("Italy", "Turin");
        deliver("USA", "Las-Vegas");
        deliver("Russia", "Saint-Petersburg");
        deliver("England", "Manchester");
        deliver("Germany", "Koln");
        deliver("Spain", "Toledo");
        deliver("Scotland", "Glasgow");
        deliver("Russia", "Krasnodar");
        deliver("Russia", "Sochi");
        deliver("Italy", "Milan");
        deliver("Germany", "Bonn");
        deliver("USA", "Los-Angels");
        deliver("France", "Paris");
        deliver("Spain", "Santader");
        deliver("Russia", "Murmansk");
        deliver("Russia", "Petrozavodsk");
        deliver("Italy", "Rimini");
        deliver("USA", "Chicago");
        deliver("Germany", "Dresden");
        deliver("USA", "New-York");
        deliver("Finland", "Helsinki");
        deliver("Russia", "Omsk");
        deliver("Spain", "Madrid");
        deliver("France", "Marcel");
        deliver("Italy", "Verona");
        deliver("Germany", "Dortmund");
        deliver("Russia", "Novosibirsk");
        deliver("Russia", "Vladivostok");
        deliver("USA", "Mayami");
        deliver("Spain", "Barcelona");

        Thread.sleep(DELAY.toMillis());

        master.close();

        System.out.println("======================= " + master.getName().toUpperCase() + " =======================");
        System.out.println("Projects:  " + System.lineSeparator() + printProjects(master.getProjects()));
        System.out.println("Failed:    " + System.lineSeparator() + printRequests(master.getFailed()));

        for (Slave slave : master.getSlaves()) {
            System.out.println("======================= " + slave.getName().toUpperCase() + " =======================");
            System.out.println("Projects:  " + System.lineSeparator() + printProjects(slave.getProjects()));
            System.out.println("Deeply equal: " +
                    String.valueOf(Objects.deepEquals(master.getProjects(), slave.getProjects())).toUpperCase());

            slave.close();
        }

        System.out.println("======================================================");

        generator.shutdown();
    }

    private static void deliver(String projectId, String data) {
        generator.execute(new DeliverTask(projectId, data));
    }

    private static String printProjects(Collection<Project> projects) {
        return projects.stream().map(Project::toString).collect(Collectors.joining(System.lineSeparator()));
    }

    private static String printRequests(Collection<Request> projects) {
        return projects.stream().map(Request::toString).collect(Collectors.joining(System.lineSeparator()));
    }

    private static class DeliverTask implements Runnable {
        private final String projectId;
        private final String data;

        DeliverTask(String projectId, String data) {
            this.projectId = projectId;
            this.data = data;
        }

        @Override
        public void run() {
            LOG.trace("Project with projectId='{}' and data='{}' is being sent.", projectId, data);
            master.postProject(projectId, data);
        }
    }
}
