package com.gmail.eksuzyan.pavel.concurrency.stores;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.tasks.SendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Master implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(Master.class);

    private final Map<Long, Slave> slaves;
    private final Map<String, Project> projects = new HashMap<>();

    private final ExecutorService dispatcher = Executors.newCachedThreadPool();

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> r1.getRepeatDate().compareTo(r2.getRepeatDate());

    private final BlockingQueue<Request> waitingRequests = new LinkedBlockingQueue<>();

    /* SHOULD BE NOT COMPARABLE BY DATE */
    private final NavigableSet<Request> failedRequests = new ConcurrentSkipListSet<>(requestDateComparator);

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

                    dispatcher.execute(task);
                }
            } catch (InterruptedException e) {
                LOG.error("Request has been lost due to unknown error.", e);
            }
        });

        Thread backWorker = new Thread(() -> {
//            Request request;
//            try {
//                while (!Thread.currentThread().isInterrupted()
//                        && (request = failedRequests.pollFirst()) != null) {
////                            && (request = failedRequests.first()) != null) {
//                    if (request.getRepeatDate().isAfter(LocalDateTime.now())) {
//                        waitingRequests.put(new Request(request, 1));
////                        failedRequests.remove(request);
//                    } else {
//                        failedRequests.add(request);
//                    }
//                }
//            } catch (InterruptedException e) {
//                LOG.error("Request has been lost due to unknown error.", e);
//            }
        });

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
                LOG.debug("Request is added to waitingRequests queue on sending to Slave #{}.", slave.id);
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
        if (!dispatcher.isShutdown()) dispatcher.shutdown();

        if (slaves != null)
            slaves.values().forEach(Slave::close);

        LOG.info("Master closed.");
    }
}
