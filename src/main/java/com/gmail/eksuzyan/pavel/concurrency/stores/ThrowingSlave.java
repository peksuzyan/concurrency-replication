package com.gmail.eksuzyan.pavel.concurrency.stores;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class ThrowingSlave extends Slave {

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        if (Math.random() > 0.2) throw new Exception();
        super.postProject(projectId, version, data);
    }

}
