package com.gmail.eksuzyan.pavel.concurrency.logic.slave;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Presents a default implementation of slave interface.
 *
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public class DefaultSlave<T> implements Slave<T> {

    /**
     * Ordinary logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(DefaultSlave.class);

    /**
     * Counter serves to generate unique slave ID.
     */
    private static AtomicLong slaveCounter = new AtomicLong();

    /**
     * Default slave name.
     */
    private static final String DEFAULT_NAME = "Slave";

    /**
     * Indicates either slave closed or not.
     */
    private volatile boolean closed = false;

    /**
     * Slave name.
     */
    private final String name;

    /**
     * Slave store serves to keep published projects.
     */
    private final ConcurrentMap<String, Project<T>> projects = new ConcurrentHashMap<>();

    /**
     * Locks access to read and write closing flag.
     */
    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    /**
     * Single constructor.
     *
     * @param name slave name
     */
    public DefaultSlave(String name) {
        long id = slaveCounter.incrementAndGet();
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", DEFAULT_NAME, id) : name;

        LOG.info("{} initialized.", getName());
    }

    /**
     * Returns slave name.
     *
     * @return slave name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Base implementation of postProject method.
     *
     * @param projectId project id
     * @param version   project version
     * @param data      project data
     */
    @Override
    public void postProject(String projectId, long version, T data) throws Exception {
        long startTime = System.currentTimeMillis();

        closeLock.readLock().lock();
        try {
            if (closed) return;
        } finally {
            closeLock.readLock().unlock();
        }

        Project<T> newProject = new Project<>(projectId, data, version);

        Project<T> oldProject = projects.putIfAbsent(projectId, newProject);

        boolean isAdded = true;
        if (oldProject != null
                && !Objects.equals(oldProject, newProject)
                && oldProject.version < newProject.version) {
            isAdded = projects.replace(projectId, oldProject, newProject);
        }

        if (!isAdded) {
            LOG.warn("Project{id='{}', version='{}', data='{}'} is being tried to insert into slave's store one more time.", projectId, version, data);
            postProject(projectId, version, data);
            return;
        }

        LOG.trace("slavePostProjectDefault: {}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * Base implementation of getProjects method.
     *
     * @return a set of projects
     */
    @Override
    public Collection<Project<T>> getProjects() {
        return new ArrayList<>(projects.values());
    }

    /**
     * Stops slave.
     */
    @Override
    public void close() {

        closeLock.readLock().lock();
        try {
            if (closed)
                throw new IllegalStateException(getName() + " closed already.");
        } finally {
            closeLock.readLock().unlock();
        }

        closeLock.writeLock().lock();
        try {
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }

        LOG.info("{} closed.", getName());
    }
}
