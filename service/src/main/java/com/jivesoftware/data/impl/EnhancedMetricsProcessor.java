package com.jivesoftware.data.impl;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;
import com.codahale.metrics.*;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.*;
import com.jivesoftware.data.DBaaSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EnhancedMetricsProcessor extends AbstractScheduledService {

    private final static Logger logger = LoggerFactory.getLogger(EnhancedMetricsProcessor.class);

    private static final String METRICS_TAG = "aws.dbaas";
    private static final String EMPTY_STRING = "";
    private static final long ONE_MINUTE = 60000;
    private static final String NO_RDS_LOG_GROUP = "phony";
    private static final String RDSOSMETRICS = "RDSOSMetrics";
    private static final String INSTANCEID = "instanceID";

    private final AWSLogsClient awsLogsClient;
    private final DBaaSConfiguration dBaaSConfiguration;
    private final MetricRegistry metricRegistry;

    @Inject
    public EnhancedMetricsProcessor(AWSLogsClient awsLogsClient,
                                    DBaaSConfiguration dBaaSConfiguration,
                                    MetricRegistry metricRegistry){

        this.awsLogsClient = awsLogsClient;
        this.dBaaSConfiguration = dBaaSConfiguration;
        this.metricRegistry = metricRegistry;
    }


    @Override
    protected void runOneIteration() throws Exception {
        long timestamp = System.currentTimeMillis();
        String currentTime = new Timestamp(timestamp).toString();
        logger.debug(String.format("Processing logs at %s", currentTime));

        LogGroup rdsOSMetrics = new LogGroup();
        rdsOSMetrics.setLogGroupName(NO_RDS_LOG_GROUP);
        OutputLogEvent logEvent;


        DescribeLogGroupsRequest logGroupsRequest = new DescribeLogGroupsRequest();
        List<LogGroup> logGroups = awsLogsClient.describeLogGroups(logGroupsRequest).getLogGroups();
        if(logGroups.isEmpty()) {
            logger.error(String.format("Cannot receive results from AWSLogsClient. Time: %s", currentTime));
            return;
        }
        for (LogGroup group : logGroups) {
            if(RDSOSMETRICS.equals(group.getLogGroupName())){
                rdsOSMetrics = group;
            }
        }
        if(rdsOSMetrics.getLogGroupName().equals(NO_RDS_LOG_GROUP)) {
            logger.error(String.format("No metrics found for log group %s", RDSOSMETRICS));
            return;
        }

        DescribeLogStreamsRequest logStreamsRequest = new DescribeLogStreamsRequest()
                .withLogGroupName(rdsOSMetrics.getLogGroupName());

        DescribeLogStreamsResult logStreamsResult = awsLogsClient.describeLogStreams(logStreamsRequest);
        List<LogStream> logStreams = logStreamsResult.getLogStreams();

        if(logStreams.isEmpty()) {
            logger.error(String.format(
                    "The RDSOSMetrics group has published nothing for the minute precluding %s",
                    currentTime));
        } else {
            for (LogStream stream : logStreams) {
                GetLogEventsRequest logEventsRequest = new GetLogEventsRequest()
                        .withLogStreamName(stream.getLogStreamName())
                        .withLogGroupName(rdsOSMetrics.getLogGroupName())
                        .withStartTime(timestamp - ONE_MINUTE)
                        .withLimit(1);
                GetLogEventsResult logEventsResult = awsLogsClient.getLogEvents(logEventsRequest);
                List<OutputLogEvent> logEventsList = logEventsResult.getEvents();
                if(logEventsList.isEmpty()) {
                    logger.error(String.format(
                            "No logs found from Amazon for the minute precluding %s", currentTime));
                    continue;
                }
                logEvent = logEventsList.get(0);
                String message = logEvent.getMessage();
                processRDSEnhancedMonitoringMessage(message);
            }
        }
    }

    void processRDSEnhancedMonitoringMessage(String logMessage) {

        JsonParser parser = new JsonParser();
        JsonObject jsonMessage = parser.parse(logMessage).getAsJsonObject();
        if(jsonMessage.has(INSTANCEID)) {
            String instanceName = jsonMessage.get(INSTANCEID).getAsString();
            parseJson(METRICS_TAG, instanceName, jsonMessage);
        }
        else {
            logger.error(String.format("Metrics no longer being received with a recognizable " +
                    "InstanceID.  Either the metric format has changed or the wrong metrics have " +
                    "been received.  Investigate immediately!"));
        }
    }

    private void parseJson(String parentName, String instanceName, JsonObject jsonObject) {
        String namespace = parentName;
        for(Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            if (INSTANCEID.equals(entry.getKey())){
                continue;
            }
            namespace = namespace + "." + entry.getKey();
            if (entry.getValue().isJsonArray()) {
                parseArray(namespace, instanceName, (JsonArray) jsonObject.get(entry.getKey()));
            }
            else {
                if (entry.getValue().isJsonObject()) {
                    parseJson(namespace, instanceName, (JsonObject) jsonObject.get(entry.getKey()));
                }
                else {
                    createGauge(namespace, instanceName, jsonObject.get(entry.getKey()));
                }
            }
        }
    }

    private void parseArray(String parentName, String instanceName, JsonArray jsonArray) {
        for(JsonElement jsonElement : jsonArray) {
            if(jsonElement.isJsonObject()) {
                parseJson(parentName, instanceName, jsonElement.getAsJsonObject());
            }
            else {
                if(jsonElement.isJsonArray()) {
                    parseArray(parentName, instanceName, jsonElement.getAsJsonArray());
                }
                else {
                    createGauge(parentName, instanceName, jsonElement);
                }
            }
        }
    }

    private void createGauge(String namespace, String instanceName, JsonElement statistic) {

        JsonPrimitive checkValue = statistic.getAsJsonPrimitive();
        String gaugeName = String.format("%s[Environment:%s,InstanceID:%s]", namespace, dBaaSConfiguration.getMakoEnvironment(), instanceName);
        if(checkValue.isNumber()) {
            metricRegistry.register(MetricRegistry.name(gaugeName), (Gauge<Integer>)()  -> checkValue.getAsInt());
        }
        if(checkValue.isString()) {
            metricRegistry.register(MetricRegistry.name(gaugeName), (Gauge<String>)()  -> checkValue.getAsString());
        }

    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedRateSchedule(0,
                dBaaSConfiguration.getMetricsPolling(), TimeUnit.SECONDS);
    }

}