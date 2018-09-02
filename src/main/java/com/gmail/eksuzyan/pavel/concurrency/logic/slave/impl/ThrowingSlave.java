package com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.slave.DefaultSlave;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class ThrowingSlave extends DefaultSlave {

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
        super.postProject(projectId, version, data);
    }
}
