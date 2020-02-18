squeuel — SQL backed Message Queue
====================================
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

Add the dependency for _squeuel_ to your pom.xml:

    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>
    <dependency>
        <groupId>com.github.benhubert.squeuel</groupId>
        <artifactId>squeuel-jdbctemplate</artifactId>
        <version>0.2.0</version>
    </dependency>

While _squeuel_ should work in any other environment too, for Spring Boot you
can configure the following beans:

    @Bean
    @Autowired
    public StorageProvider squeuelStorageProvider(DataSource dataSource) {
        return new JdbcStorageProvider(dataSource, "squeuel_event", "squeuel_event_lock");
    }
    
    @Bean
    @Autowired
    public QueueService squeuelQueueService(StorageProvider storageProvider) {
        return new DefaultQueueService(storageProvider);
    }

If you want to monitor _squeuel_'s activity, this library also provides an
integration for Micrometer:

    @Bean
    @Autowired
    public SqueuelMeterBinder squeuelMeterBinder(QueueService queueService) {
        return new SqueuelMeterBinder(queueService);
    }

Now that you have your `QueueService` ready, you can use it in your applications
code. To push events to a queue, simply call

    queueService.enqueue(queueName, partition, serializedData);

To handle enqueued events, use

    queueService.handleNext(queueName, batchSize, maxLockTime, this::handlePersistedEvent);

To clean up old and processed events, use

    squeuelEventQueue.cleanup(queue, removeUntilUtc);

Squeuel does not provide any scheduling mechanism. Use the scheduling mechanism
of your choice to call the above methods periodically.
