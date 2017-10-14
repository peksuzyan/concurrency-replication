package com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.AbstractSlave;

import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class HealthySlave extends AbstractSlave {

    public HealthySlave() {
        this(null);
    }

    public HealthySlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        postProjectDefault(projectId, version, data);
    }

    @Override
    public Collection<Project> getProjects() {
        return getProjectsDefault();
    }

    @Override
    public void close() {
        shutdownDefault();
    }
}
