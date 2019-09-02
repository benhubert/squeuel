package at.benjaminhubert.squeuel.testutils;

import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseTables {

    private static final AtomicInteger INDEX = new AtomicInteger();

    private final String prefix;

    public DatabaseTables() {
        this.prefix = "test_" + INDEX.incrementAndGet();
    }

    public String event() {
        return this.prefix + "_event";
    }

    public String lock() {
        return this.prefix + "_lock";
    }

}
