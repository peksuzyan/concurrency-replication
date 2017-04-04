package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.tasks.SendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Presents a base implementation of master interface.
 *
 * @author Pavel Eksuzian.
 *         Created: 04.04.2017.
 */
public abstract class AbstractMaster implements Master {

    /**
     * Ordinary logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(AbstractMaster.class);

    /**
     * Counter serves to generate unique master ID.
     */
    private static long masterCounter = 0L;

    /**
     * Default master name.
     */
    private static final String defaultName = "Master";

    /**
     * Master name.
     */
    private String name;

    private static final int INITIAL_QUEUE_CAPACITY = 10;

    private final Map<String, Slave> slaves;

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

    /**
     * Single constructor.
     *
     * @param name   master name
     * @param slaves slaves
     */
    protected AbstractMaster(String name, Slave... slaves) {
        long id = ++masterCounter;
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", defaultName, id) : name;

        try {
            this.slaves = (slaves == null || slaves.length == 0)
                    ? Collections.emptyMap()
                    : Collections.unmodifiableMap(
                            Arrays.stream(slaves).collect(Collectors.toMap(Slave::getName, slave -> slave)));
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        }

        initWorkers();

        LOG.info("{} initialized.", getName());
    }

    private void initWorkers() {

        postWorker = new Thread(() -> {
            Request request;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = waitingRequests.take();

                    Runnable task = new SendingTask(
                            slaves.get(request.getSlave()), request, failedRequests);

                    if (!dispatcher.isShutdown()) dispatcher.execute(task);
                    else waitingRequests.put(request);
                }
            } catch (InterruptedException e) {
                LOG.info("{} PostWorker has been collapsed due to thread interruption.", getName());
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

                LOG.info("{} BackWorker has been collapsed due to thread interruption.", getName());
            }
        });

        postWorker.setName(getName() + "-postThread");
        backWorker.setName(getName() + "-backThread");

        postWorker.start();
        backWorker.start();
    }

    /**
     * Posts project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    protected void postProjectV(String projectId, String data) {
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
                waitingRequests.put(new Request(slave.getName(), project));
            } catch (InterruptedException e) {
                LOG.error("Main thread has been interrupted unexpectedly!", e);
            }
        });
    }

    /**
     * Returns stored projects.
     *
     * @return projects
     */
    protected Collection<Project> getProjectsV() {
        return projects.values();
    }

    /**
     * Returns failed requests.
     *
     * @return requests
     */
    protected Collection<Request> getFailedV() {
        Stream<Request> waitingFailedRequests =
                waitingRequests.stream().filter(request -> request.getAttempt() > 1);
        return Stream
                .concat(waitingFailedRequests, failedRequests.stream())
                .collect(Collectors.toList());
    }

    /**
     * Stops workers and thread pool.
     */
    protected void shutdown() {
        dispatcher.shutdown();

        postWorker.interrupt();
        backWorker.interrupt();

        try {
            while (!dispatcher.awaitTermination(15, TimeUnit.SECONDS)) {
                LOG.info("{}: Dispatcher tasks haven't been yet completed.", getName());
            }
        } catch (InterruptedException e) {
            LOG.error("Main thread has been interrupted unexpectedly!", e);
        }

        LOG.info("{} closed.", getName());
    }

    /**
     * Sets master name.
     *
     * @param name master name
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns master name.
     *
     * @return master name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns slaves which are related to this master instance.
     *
     * @return a set of slaves
     */
    @Override
    public Collection<Slave> getSlaves() {
        return slaves.values();
    }
}
