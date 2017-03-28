package com.gmail.eksuzyan.pavel.concurrency.stores;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Slave implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(Slave.class);

    private static final AtomicLong counter = new AtomicLong(0L);

    public final long id;

    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    public Slave() {
        this.id = counter.incrementAndGet();

        LOG.info("Slave #{} initialized.", id);
    }

    public void postProject(String projectId, long version, String data) throws Exception {

        Project newProject = new Project(projectId, data, version);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        if (oldProject != null
                && !Objects.equals(oldProject, newProject)
                && oldProject.getVersion() < newProject.getVersion()) {
            projects.replace(projectId, oldProject, newProject);
        }

    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    @Override
    public void close() {
        LOG.info("Slave #{} closed.", id);
    }
}
