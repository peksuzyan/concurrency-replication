package com.gmail.eksuzyan.pavel.concurrency.tasks;

import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.stores.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;

/**
 * @author Pavel Eksuzian.
 *         Created: 19.03.2017.
 */
public class SendingTask implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(SendingTask.class);

    private static final int SLEEP_TIME_BEFORE_SEND = 500;

    private final Slave slave;
    private final Request request;
    private final BlockingQueue<Request> failedRequests;

    public SendingTask(Slave slave, Request request, BlockingQueue<Request> failedRequests) {
        this.slave = slave;
        this.request = request;
        this.failedRequests = failedRequests;
    }

    @Override
    public void run() {
        try {
            while (!request.getRepeatDate().isBefore(LocalDateTime.now()))
                Thread.sleep(SLEEP_TIME_BEFORE_SEND);

            slave.postProject(
                    request.getProject().getId(),
                    request.getProject().getVersion(),
                    request.getProject().getData());

            LOG.debug("[+] => Slave #{} => {}.", request.getSlaveId(), request);
        } catch (Throwable e) {
            LOG.debug("[-] => Slave #{} => {}.", request.getSlaveId(), request);

            try {
                failedRequests.put(new Request(request, 1));
                LOG.debug("[+] => failedRequests => {}", request);
            } catch (InterruptedException ex) {
                LOG.error("Request has been lost due to unknown error.", ex);
            }
        }
    }
}
