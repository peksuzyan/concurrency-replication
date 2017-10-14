package com.gmail.eksuzyan.pavel.concurrency.util.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Pavel Eksuzian.
 *         Created: 14.10.2017.
 */
@SuppressWarnings("UnusedReturnValue")
public class Status implements StatusMBean, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Status.class);

    private static final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private final ObjectName mBeanName;

    private final AtomicLong replicatedProjects = new AtomicLong();
    private final AtomicLong registeredRequests = new AtomicLong();

    public Status() {
        try {
            mBeanName = new ObjectName(
                    this.getClass().getPackage() + ":type=" + this.getClass().getSimpleName());

            if (!mBeanServer.isRegistered(mBeanName))
                mBeanServer.registerMBean(this, mBeanName);

            LOG.info("Status MBean is registered successfully.");
        } catch (InstanceAlreadyExistsException
                | MBeanRegistrationException
                | NotCompliantMBeanException
                | MalformedObjectNameException e) {
            throw new IllegalStateException("Status MBean isn't registered.", e);
        }
    }

    @Override
    public void close() {
        if (mBeanServer.isRegistered(mBeanName))
            try {
                mBeanServer.unregisterMBean(mBeanName);
                LOG.info("Status MBean is unregistered successfully.");
            } catch (InstanceNotFoundException | MBeanRegistrationException e) {
                throw new IllegalStateException("Status MBean isn't unregistered.", e);
            }
    }

    public long incAndGetReplicatedProjects() {
        return replicatedProjects.incrementAndGet();
    }

    public long addAndGetRegisteredRequests(long delta) {
        return registeredRequests.addAndGet(delta);
    }

    public long decAndGetRegisteredRequests() {
        return registeredRequests.decrementAndGet();
    }

    @Override
    public long getRegisteredRequests() {
        return registeredRequests.get();
    }

    @Override
    public long getReplicatedProjects() {
        return replicatedProjects.get();
    }
}
