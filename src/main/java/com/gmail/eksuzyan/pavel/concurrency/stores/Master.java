package com.gmail.eksuzyan.pavel.concurrency.stores;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.tasks.SendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Master implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(Master.class);

    private static final int INITIAL_QUEUE_CAPACITY = 10;

    public final Map<Long, Slave> slaves;

    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    private final ExecutorService dispatcher = Executors.newCachedThreadPool();

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> r1.getRepeatTime().compareTo(r2.getRepeatTime());

    private final BlockingQueue<Request> waitingRequests =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Request> failedRequests =
            new PriorityBlockingQueue<>(INITIAL_QUEUE_CAPACITY, requestDateComparator);

    private Thread postWorker;
    private Thread backWorker;

    public Master(Slave... slaves) {
        this.slaves = Collections.unmodifiableMap(
                Arrays.stream(slaves).collect(Collectors.toMap(slave -> slave.id, slave -> slave)));

        initWorkers();

        LOG.info("Master initialized.");
    }

    private void initWorkers() {

        postWorker = new Thread(() -> {
            Request request;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = waitingRequests.take();

                    Runnable task = new SendingTask(
                            slaves.get(request.getSlaveId()), request, failedRequests);

                    if (!dispatcher.isShutdown()) dispatcher.execute(task);
                    else waitingRequests.put(request);
                }
            } catch (InterruptedException e) {
                LOG.info("PostWorker has been collapsed due to thread interruption.");
            }
        });

        backWorker = new Thread(() -> {
            Request request = null;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = null;

                    request = failedRequests.take();

                    waitingRequests.put(request);
                }
            } catch (InterruptedException e) {
                if (request != null)
                    try {
                        failedRequests.put(request);
                    } catch (InterruptedException ex) {
                        LOG.error("Request has been lost due to unknown error.", ex);
                    }

                LOG.info("BackWorker has been collapsed due to thread interruption.");
            }
        });

        postWorker.setName("postThread");
        backWorker.setName("backThread");

        postWorker.start();
        backWorker.start();

        LOG.info("Workers initialized.");
    }

    public void postProject(String projectId, String data) {

        Project newProject = new Project(projectId, data);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        if (oldProject == null) {
            postProjectToSlaves(newProject);
        } else {
            newProject = oldProject.setDataAndIncVersion(data);
            boolean isReplaced = projects.replace(projectId, oldProject, newProject);
            if (isReplaced) {
                postProjectToSlaves(newProject);
            } else {
                postProject(projectId, data);
            }
        }
    }

    private void postProjectToSlaves(final Project project) {
        slaves.values().forEach(slave -> {
            try {
                waitingRequests.put(new Request(slave.id, project));
            } catch (InterruptedException e) {
                LOG.error("Main thread has been interrupted unexpectedly!", e);
            }
        });
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public Collection<Request> getFailed() {
        Stream<Request> waitingFailedRequests =
                waitingRequests.stream().filter(request -> request.getAttempt() > 1);
        return Stream
                .concat(waitingFailedRequests, failedRequests.stream())
                .collect(Collectors.toList());
    }

    @Override
    public void close() {

        dispatcher.shutdown();

        postWorker.interrupt();
        backWorker.interrupt();

        try {
            while (!dispatcher.awaitTermination(15, TimeUnit.SECONDS)) {
                LOG.info("Dispatcher tasks haven't been yet completed.");
            }
        } catch (InterruptedException e) {
            LOG.error("Main thread has been interrupted unexpectedly!", e);
        }

        if (slaves != null)
            slaves.values().forEach(Slave::close);

        LOG.info("Master closed.");
    }
}
