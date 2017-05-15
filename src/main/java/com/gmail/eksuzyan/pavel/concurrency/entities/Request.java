package com.gmail.eksuzyan.pavel.concurrency.entities;

import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;

import java.util.Objects;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Request {

    public final Project project;
    public final Slave slave;
    public final int attempt;
    public final int code;
    public final long repeatDate;

    private static final int FIRST_ATTEMPT = 1;
    private static final int SUCCESS = 0;

    public Request(Slave slave, Project project) {
        this(slave, project, FIRST_ATTEMPT, SUCCESS);
    }

    private Request(Slave slave, Project project, int attempt, int code) {
        this.project = project;
        this.slave = slave;
        this.attempt = attempt;
        this.code = code;
        this.repeatDate = Long.sum(
                attempt > FIRST_ATTEMPT ? 1 << (attempt + 5) : 0L,
                System.currentTimeMillis());
    }

    public Request setCodeAndIncAttempt(int code) {
        return new Request(slave, project, attempt + 1, code);
    }

    @Override
    public String toString() {
        return "Request{" +
                "project=" + project +
                ", slave='" + slave + '\'' +
                ", attempt=" + attempt +
                ", code=" + code +
                ", repeatDate=" + repeatDate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Request)) return false;
        Request request = (Request) o;
        return attempt == request.attempt &&
                code == request.code &&
                repeatDate == request.repeatDate &&
                Objects.equals(project, request.project) &&
                Objects.equals(slave, request.slave);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, slave, attempt, code, repeatDate);
    }
}
