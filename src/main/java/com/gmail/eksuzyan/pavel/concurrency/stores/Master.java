package com.gmail.eksuzyan.pavel.concurrency.stores;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.tasks.SendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Master implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(Master.class);

    private static final int INITIAL_QUEUE_CAPACITY = 10;

    public final Map<Long, Slave> slaves;

    private final Map<String, Project> projects = new HashMap<>();

    private final ExecutorService dispatcher = Executors.newCachedThreadPool();

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> r1.getRepeatDate().compareTo(r2.getRepeatDate());

    private final BlockingQueue<Request> waitingRequests =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Request> failedRequests =
            new PriorityBlockingQueue<>(INITIAL_QUEUE_CAPACITY, requestDateComparator);

    public Master(Slave... slaves) {
        this.slaves = Arrays.stream(slaves)
                .collect(Collectors.toMap(slave -> slave.id, slave -> slave));

        initWorkers();

        LOG.info("Master initialized.");
    }

    private void initWorkers() {

        Thread postWorker = new Thread(() -> {
            Request request;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = waitingRequests.take();

                    Runnable task = new SendingTask(
                            slaves.get(request.getSlaveId()), request, failedRequests);

                    if (!dispatcher.isShutdown()) dispatcher.execute(task);
                }
            } catch (InterruptedException e) {
                LOG.error("Request has been lost due to unknown error.", e);
            }
        });

        Thread backWorker = new Thread(() -> {
            Request request;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = failedRequests.take();

                    waitingRequests.put(request);
                }
            } catch (InterruptedException e) {
                LOG.error("Request has been lost due to unknown error.", e);
            }
        });

        postWorker.setName("postThread");
        backWorker.setName("backThread");

        postWorker.setDaemon(true);
        backWorker.setDaemon(true);

        postWorker.start();
        backWorker.start();

        LOG.info("Workers initialized.");
    }

    public void postProject(String projectId, String data) {
        final Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, projects.get(projectId).getVersion() + 1L);
        projects.put(projectId, project);

        slaves.values().forEach(slave -> {
            try {
                waitingRequests.put(new Request(slave.id, project));
            } catch (InterruptedException e) {
                LOG.error("Request has been lost due to unknown error.", e);
            }
        });
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public Collection<Request> getFailed() {
        return failedRequests;
    }

    @Override
    public void close() {

        dispatcher.shutdown();

        boolean isDone = false;

        try {
            isDone = dispatcher.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.error("Request has been lost due to unknown error.", e);
        }

        LOG.info("Have dispatcher tasks been already completed? {}", isDone);

        if (slaves != null)
            slaves.values().forEach(Slave::close);

        LOG.info("Master closed.");
    }
}
