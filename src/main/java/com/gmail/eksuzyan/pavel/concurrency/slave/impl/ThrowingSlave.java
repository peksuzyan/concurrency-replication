package com.gmail.eksuzyan.pavel.concurrency.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.slave.AbstractSlave;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class ThrowingSlave extends AbstractSlave {

    public ThrowingSlave() {
        this(null);
    }

    public ThrowingSlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        if (Math.random() > 0.2) throw new Exception();
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
