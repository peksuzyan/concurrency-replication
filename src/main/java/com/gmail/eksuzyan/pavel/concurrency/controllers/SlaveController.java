package com.gmail.eksuzyan.pavel.concurrency.controllers;

import com.gmail.eksuzyan.pavel.concurrency.entity.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class SlaveController implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(SlaveController.class);

    private static long lastId = 0L;
    private final long id;

    private final Map<String, Project> projects = new HashMap<>();

    public SlaveController() {
        this.id = ++lastId;

        LOG.info("Slave #{} initialized.", id);
    }

    public int postProject(String projectId, long version, String data) {
        if ((int) (Math.random() * 10) > 2) return 1;

        Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, version);
        projects.put(projectId, project);

        return 0;
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public long getId() {
        return id;
    }

    @Override
    public void close() {
        /* NOP */

        LOG.info("Slave #{} closed.", id);
    }
}
