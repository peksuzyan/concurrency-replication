package com.gmail.eksuzyan.pavel.concurrency.controllers;

import com.gmail.eksuzyan.pavel.concurrency.entity.Project;
import com.gmail.eksuzyan.pavel.concurrency.entity.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class MasterController implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(MasterController.class);

    private final Map<Long, SlaveController> slaves;

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> r1.getRepeatDate().compareTo(r2.getRepeatDate());

    private final Map<String, Project> projects = new HashMap<>();
    private final NavigableSet<Request> failedRequests = new ConcurrentSkipListSet<>(requestDateComparator);
    private final BlockingQueue<Request> waitingRequests = new LinkedBlockingQueue<>();

    private boolean isStopped = false;

    public MasterController(SlaveController... slaves) {
        this.slaves = Arrays.stream(slaves)
                .collect(Collectors.toMap(SlaveController::getId, slave -> slave));

        initWorkers();

        LOG.info("Master initialized.");
    }

    private void initWorkers() {
        Thread postman = new Thread(() -> {
            Request request;
            try {
                while (!isStopped && !Thread.currentThread().isInterrupted()) {
                    if ((request = waitingRequests.take()) != null) {
                        Project project = request.getProject();
                        SlaveController slave = slaves.get(request.getSlaveId());
                        int code = slave.postProject(project.getId(), project.getVersion(), project.getData());
                        if (code != 0) {
                            failedRequests.add(new Request(request, code));
                            LOG.debug("[FAILURE] {} hasn't been sent.", request);
                        } else {
                            LOG.debug("[SUCCESS] {} has been sent.", request);
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("SEND worker has been interrupted.");
            }
        }, "send");

        Thread repeater = new Thread(() -> {
            try {
                while (!isStopped && !Thread.currentThread().isInterrupted()) {
                    if (!failedRequests.isEmpty()) {
                        Request request = failedRequests.pollFirst();
                        if (request.getRepeatDate().isBefore(LocalDateTime.now())) {
                            failedRequests.remove(request);
                            waitingRequests.put(request);
                            LOG.debug("[MOVE] {} has been moved on sending.", request);
                        } else {
                            failedRequests.add(request);
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("BACK worker has been interrupted.");
            }
        }, "back");

        postman.start();
        repeater.start();

        LOG.info("SEND thread started.");
        LOG.info("BACK thread started.");
    }

    public void postProject(String projectId, String data) {
        Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, projects.get(projectId).getVersion() + 1L);
        projects.put(projectId, project);

        slaves.keySet().forEach(slaveId -> {
            Request request = new Request(slaveId, project);
            try {
                waitingRequests.put(request);
                LOG.debug("[PUT] {} will be sent.", request);
            } catch (InterruptedException e) {
                LOG.error("MAIN thread has been interrupted.");
            }
        });
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public Collection<Request> getFailed() {
        return failedRequests;
    }

    public Collection<Request> getWaiting() {
        return waitingRequests;
    }

    @Override
    public void close() {
        isStopped = true;

        if (slaves != null)
            slaves.values().forEach(SlaveController::close);

        LOG.info("Master closed.");
    }
}
