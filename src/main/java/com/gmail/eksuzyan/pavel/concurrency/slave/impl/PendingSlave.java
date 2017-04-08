package com.gmail.eksuzyan.pavel.concurrency.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.slave.AbstractSlave;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class PendingSlave extends AbstractSlave {

    public PendingSlave() {
        this(null);
    }

    public PendingSlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        Thread.sleep((long) (Math.random() * 10000));
        postProjectDefault(projectId, version, data);
    }

    @Override
    public Collection<Project> getProjects() {
        return getProjectsDefault();
    }

    @Override
    public void close() throws IOException {
       /* NOP */
    }
}
