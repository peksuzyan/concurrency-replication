package com.gmail.eksuzyan.pavel.concurrency.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.slave.AbstractSlave;

import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class ThrowingSlave extends AbstractSlave {

    private final double limit;

    public ThrowingSlave() {
        this(null, 0.2);
    }

    public ThrowingSlave(String name, double limit) {
        super(name);
        this.limit = limit;
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        if (Math.random() > limit) throw new Exception();
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
