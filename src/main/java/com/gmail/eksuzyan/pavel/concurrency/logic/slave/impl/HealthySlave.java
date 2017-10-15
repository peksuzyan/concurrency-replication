package com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.slave.DefaultSlave;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class HealthySlave extends DefaultSlave {

    public HealthySlave() {
        this(null);
    }

    public HealthySlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        super.postProject(projectId, version, data);
    }
}
