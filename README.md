SQL backed Message Queue
==========================
A simple, lightweight, reliable event queue which persists and coordinates all
events in a any standard SQL database.

_Simple_, because it doesn't aim to be a full-featured JMS implementation or
Kafka replacement. Instead it concentrates on storing a text message in a queue
and providing a method for polling these queues.

_Lightweigt_, because it comes with only a small set of essential dependencies
and should fit for almost every project setup.

_Reliable_, because it uses SQL standard mechanisms for synchronizing multiple
subscribers and ensures chronological processing for events grouped together in
the same partition.

_Persistent_, because events and their locks are stored in a database. Locks
expire after a configurable time, meaning that every event will eventually be
processed.

Refer to the javadoc of QueueService for more details about all features.

What this project is not
--------------------------
This is _not_ a JMS implementation. It's intended to be much simpler.

While it can deal with multiple instances, it is _not_ intended to be used by
high performance 

This is _not_ a blazing fast queue. Depending on the database performance, on
the usage in your application and on the number of workers handling events, it
might handle some tens of events per second. In some cases even up to 100 events
per second, but don't expect much more from it. 

This library does _not_ include any scheduling. It only provides methods for
polling a database table. It is on the developer using this library, to care
about how and how often this polling is performed.

Getting started
-----------------
Create two tables with the following fields in your database:

    CREATE TABLE squeuel_events (
        id BIGSERIAL PRIMARY KEY,
        queue VARCHAR(64) NOT NULL,
        partition VARCHAR(64) NOT NULL,
        created_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        data TEXT,
        processed BOOLEAN NOT NULL
    );
    CREATE TABLE squeuel_locks (
        id BIGSERIAL PRIMARY KEY,
        queue VARCHAR(64) NOT NULL,
        partition VARCHAR(64) NOT NULL,
        locked_until_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        UNIQUE (queue, partition) 
    );

Create your instance of `QueueService`. For a Spring application, register the
following Spring beans in your context:

    @Bean
    public JdbcTemplate squeuelJdbcTemplate() {
        // Create your instance of the JDBC template which is connected to your
        // database.
    }
    
    @Bean
    @Autowired
    public StorageProvider squeuelStorageProvider(JdbcTemplate jdbcTemplate) {
        return new JdbcStorageProvider(jdbcTemplate, "squeuel_events", "squeuel_locks");
    }
    
    @Bean
    @Autowired
    public QueueService squeuelQueueService(StorageProvider storageProvider) {
        return new DefaultQueueService(storageProvider);
    }
