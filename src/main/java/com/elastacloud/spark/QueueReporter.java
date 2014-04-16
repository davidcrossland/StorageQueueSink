package com.elastacloud.spark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;


import java.util.Locale;
import java.util.SortedMap;

/**
 * A reporter which sends a message to a storage queue when a job has been completed
 */
public class QueueReporter extends ScheduledReporter {


    /**
     * Returns a new {@link QueueReporterBuilder} for {@link QueueReporter}.
     *
     * @param registry the registry to report
     * @return a {@link QueueReporterBuilder} instance for a {@link QueueReporter}
     */
    public static QueueReporterBuilder forRegistry(MetricRegistry registry) {
        return new QueueReporterBuilder(registry);
    }

    /**
     * A builder for {@link QueueReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class QueueReporterBuilder {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private QueueReporterBuilder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public QueueReporterBuilder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public QueueReporterBuilder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Builds a {@link QueueReporter} with the given properties,
         *
         * @param connectionString the connectionString for the queue
         * @return a {@link QueueReporter}
         */
        public QueueReporter build(String connectionString, String queueName, String clusterName) {
            return new QueueReporter(registry,
                    connectionString,
                    queueName,
                    clusterName,
                    locale,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueReporter.class);

    private final String connectionString;
    private final String queueName;
    private final String clusterName;
    private final Locale locale;
    private final Clock clock;
    private boolean hasHadActiveStages = false;
    private StorageQueueWriter sbm;
     private QueueReporter(MetricRegistry registry,
                          String connectionString,
                          String queueName,
                          String clusterName,
                          Locale locale,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          Clock clock,
                          MetricFilter filter) {

        super(registry, "azurestoragequeue-reporter", filter, rateUnit, durationUnit);

        this.connectionString = connectionString;
        this.queueName = queueName;
        this.clusterName = clusterName;
        this.locale = locale;
        this.clock = clock;
        this.sbm = new StorageQueueWriter(connectionString, queueName);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        //final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        //Get the current metrics related to jobs and stages
        int allJobs = getValueForKey(gauges, "allJobs");
        int activeJobs = getValueForKey(gauges, "activeJobs");
        int failedStages = getValueForKey(gauges, "failedStages");
        int runningStages = getValueForKey(gauges, "runningStages");
        int waitingStages = getValueForKey(gauges, "waitingStages");

        //allJobs can be set to 1 before any jobs have actually been submitted.
        if(activeJobs > 0)
            hasHadActiveStages = true;

        //send a message if the job has had active stages, and no job is running/waiting
        if(hasHadActiveStages && allJobs > 0 && activeJobs == 0 && runningStages == 0 && waitingStages == 0)
        {
            LOGGER.info("Detected finished jobs - has failures="+(failedStages > 0));

            try {
                //send a complete with failure message if the failed stages > 0
                if(failedStages > 0 )
                    //we can send a delete message with job failure
                    sbm.sendMessage(clusterName + ",false");
                else //send success message
                    sbm.sendMessage(clusterName + ",true");

            }catch (Exception e)
            {
                LOGGER.error(e.getMessage());
            }

            hasHadActiveStages = false;
        }
    }

    /**
     * returns a value for a given key
     *
     * @param map the map from which a value should be retrieved
     * @param keySearchStr a key search string
     * @return The value retrieved
     */
    private int getValueForKey(Map<String, Gauge> map, String keySearchStr)
    {
        List<String> keys = new ArrayList<String>(map.keySet());

        if(keys.size() == 0)
            return -1;

         String key = findKey(keys, keySearchStr);

        if(key == null)
            return -1;

         return Integer.parseInt(map.get(key).getValue().toString());
    }

    /**
     * Attempts to match a key against a value
     *
     * @param keys the keys to search
     * @param term the value to find
     * @return The value retrieved
     */
    private String findKey(List<String> keys, String term)
    {
        for(String key : keys)
        {
            if(key.endsWith(term))
                return key;
        }

        return null;
    }

}
