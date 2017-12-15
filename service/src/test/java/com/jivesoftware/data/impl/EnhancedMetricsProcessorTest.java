package com.jivesoftware.data.impl;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jivesoftware.data.DBaaSConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class EnhancedMetricsProcessorTest {

    private EnhancedMetricsProcessor enhancedMetricsProcessor;

    private EnhancedMetricsProcessor enhancedMetricsProcessorSpy;

    @Mock
    private AWSLogsClient awsLogsClient;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private MetricRegistry metricRegistry;

    @Mock
    private DescribeLogGroupsResult logGroupsResult;

    @Mock
    private LogGroup logGroup1;

    @Mock
    private LogGroup logGroup2;

    @Mock
    private DescribeLogStreamsResult logStreamsResult;

    @Mock
    private LogStream logStream1;

    @Mock
    private LogStream logStream2;

    @Mock
    private GetLogEventsResult logEventsResult;

    @Mock
    private OutputLogEvent logEvent;

    private JsonObject jsonTestValue = new JsonObject();
    private Gson gson = new Gson();

    ArgumentCaptor<String> metricNameCaptor;
    ArgumentCaptor<Gauge> gaugeArgumentCaptor;

    Integer testValue = 42;
    String testName = "DBaaS";
    Integer[] testIntArray = {0,1,2};
    String[] testStringArray = {"un", "deux", "trois"};

    @Before
    public void setUp() {

        jsonTestValue.addProperty("instanceID", "instanceID");
        //jsonWInt.addProperty("number", testValue);
        //jsonWInt.addProperty("instanceID", "instanceID");
        //jsonWString.addProperty("name", testName);
        //jsonWString.addProperty("instanceID", "instanceID");
        //jsonWIntArray.add("array", gson.toJsonTree(testIntArray));
        //jsonWIntArray.addProperty("instanceID", "instanceID");
        //jsonWStringArray.add("array", gson.toJsonTree(testStringArray));
        //jsonWStringArray.addProperty("instanceID", "instanceID");
        //jsonNestedWInt.add("nesting", jsonWInt);
        //jsonNestedWInt.addProperty("instanceID", "instanceID");
        //jsonNestedWString.add("nesting", jsonWString);
        //jsonNestedWString.addProperty("instanceID", "instanceID");
        //jsonNestedArrWInt.add("nesting", jsonWIntArray);
        //jsonNestedArrWInt.addProperty("instanceID", "instanceID");
        //jsonNestedArrWString.add("nesting", jsonWStringArray);
        //jsonNestedArrWString.addProperty("instanceID", "instanceID");
        //jsonNestedArrInObj.add("topLevel", jsonNestedArrWString);
        //jsonNestedArrInObj.addProperty("instanceID", "instanceID");

        //jsonNestedObjInArr.addProperty("instanceID", "instanceID");

        when(dBaaSConfiguration.getMakoEnvironment()).thenReturn("test");

        when(awsLogsClient.describeLogGroups(any())).thenReturn(logGroupsResult);
        enhancedMetricsProcessor = new EnhancedMetricsProcessor(awsLogsClient, dBaaSConfiguration,
                metricRegistry);

        metricNameCaptor = ArgumentCaptor.forClass(String.class);
        gaugeArgumentCaptor = ArgumentCaptor.forClass(Gauge.class);
    }

    private void standardSetUp() {
        when(logGroup1.getLogGroupName()).thenReturn("RDSOSMetrics");
        when(logGroupsResult.getLogGroups()).thenReturn(ImmutableList.of(logGroup1, logGroup2));
        when(logGroup1.getLogGroupName()).thenReturn("Not the metrics you're looking for");
        when(logGroup2.getLogGroupName()).thenReturn("RDSOSMetrics");
        when(awsLogsClient.describeLogStreams(any())).thenReturn(logStreamsResult);
        when(logStreamsResult.getLogStreams()).thenReturn(ImmutableList.of(logStream1, logStream2));
        when(logStream2.getLogStreamName()).thenReturn("name");
        when(awsLogsClient.getLogEvents(any())).thenReturn(logEventsResult);
        when(logEventsResult.getEvents()).thenReturn(ImmutableList.of(logEvent));

    }

    @Test
    public void noLogGroupsFoundTest() throws Exception {
        when(logGroupsResult.getLogGroups()).thenReturn(ImmutableList.of());
        enhancedMetricsProcessor.runOneIteration();
        verify(awsLogsClient, times(0)).describeLogStreams(any());
    }

    @Test
    public void noRDSOSMetricsTest() throws Exception {
        when(logGroupsResult.getLogGroups()).thenReturn(ImmutableList.of(logGroup1));
        when(logGroup1.getLogGroupName()).thenReturn("Not the metrics you're looking for");
        enhancedMetricsProcessor.runOneIteration();
        verify(awsLogsClient, times(0)).describeLogStreams(any());
    }

    @Test
    public void noLogStreamsFoundTest() throws Exception {
        when(logGroupsResult.getLogGroups()).thenReturn(ImmutableList.of(logGroup1));
        when(logGroup1.getLogGroupName()).thenReturn("RDSOSMetrics");
        when(awsLogsClient.describeLogStreams(any())).thenReturn(logStreamsResult);
        when(logStreamsResult.getLogStreams()).thenReturn(ImmutableList.of());
        enhancedMetricsProcessor.runOneIteration();
        verify(awsLogsClient, times(0)).getLogEvents(any());
    }

    @Test
    public void noCurrentLogEventsTest() throws Exception {
        enhancedMetricsProcessorSpy = spy(enhancedMetricsProcessor);
        when(logGroupsResult.getLogGroups()).thenReturn(ImmutableList.of(logGroup1));
        when(logGroup1.getLogGroupName()).thenReturn("RDSOSMetrics");
        when(awsLogsClient.describeLogStreams(any())).thenReturn(logStreamsResult);
        when(logStreamsResult.getLogStreams()).thenReturn(ImmutableList.of(logStream1));
        when(logStream1.getLogStreamName()).thenReturn("name");
        when(awsLogsClient.getLogEvents(any())).thenReturn(logEventsResult);
        when(logEventsResult.getEvents()).thenReturn(ImmutableList.of());
        enhancedMetricsProcessorSpy.runOneIteration();
        verify(enhancedMetricsProcessorSpy, times(0)).processRDSEnhancedMonitoringMessage(any());
    }

    @Test
    public void skipInstanceIDMetricTest() throws Exception {
        standardSetUp();
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(0)).register(any(), any());
    }

    @Test
    public void nonNestedIntTest() throws Exception {
        standardSetUp();
        jsonTestValue.addProperty("number", testValue);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(2)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedGauges.get(0).getValue(), testValue);
        assertEquals(capturedGauges.get(1).getValue(), testValue);

    }

    @Test
    public void nonNestedStringTest() throws Exception {
        standardSetUp();
        jsonTestValue.addProperty("name", testName);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(2)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.name[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.name[Environment:test,InstanceID:instanceID]");
        assertEquals(testName, capturedGauges.get(0).getValue());
        assertEquals(testName, capturedGauges.get(1).getValue());
    }

    @Test
    public void nonNestedStringArrayTest() throws Exception {
        standardSetUp();
        jsonTestValue.add("array", gson.toJsonTree(testStringArray));
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(6)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(2), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(3), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(4), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(5), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(testStringArray[0], capturedGauges.get(0).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(1).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(2).getValue());
        assertEquals(testStringArray[0], capturedGauges.get(3).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(4).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(5).getValue());
    }

    @Test
    public void nonNestedIntArrayTest() throws Exception {
        standardSetUp();
        jsonTestValue.add("array", gson.toJsonTree(testIntArray));
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(6)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(2), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(3), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(4), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(5), "aws.dbaas.array[Environment:test,InstanceID:instanceID]");
        assertEquals(testIntArray[0], capturedGauges.get(0).getValue());
        assertEquals(testIntArray[1], capturedGauges.get(1).getValue());
        assertEquals(testIntArray[2], capturedGauges.get(2).getValue());
        assertEquals(testIntArray[0], capturedGauges.get(3).getValue());
        assertEquals(testIntArray[1], capturedGauges.get(4).getValue());
        assertEquals(testIntArray[2], capturedGauges.get(5).getValue());

    }

    @Test
    public void nestedObjectIntTest() throws Exception {
        standardSetUp();
        JsonObject jsonWInt = new JsonObject();
        jsonWInt.addProperty("number", testValue);
        jsonTestValue.add("nesting", jsonWInt);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(2)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.nesting.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.nesting.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedGauges.get(0).getValue(), testValue);
        assertEquals(capturedGauges.get(1).getValue(), testValue);
    }

    @Test
    public void nestedObjectStringTest() throws Exception {
        standardSetUp();
        JsonObject jsonWString = new JsonObject();
        jsonWString.addProperty("name", testName);
        jsonTestValue.add("nesting", jsonWString);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(2)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.nesting.name[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.nesting.name[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedGauges.get(0).getValue(), testName);
        assertEquals(capturedGauges.get(1).getValue(), testName);
    }

    @Test
    public void nestedArrayIntTest() throws Exception {
        standardSetUp();
        JsonObject jsonWIntArray = new JsonObject();
        jsonWIntArray.add("array", gson.toJsonTree(testIntArray));
        jsonTestValue.add("nesting", jsonWIntArray);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(6)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(2), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(3), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(4), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(5), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(testIntArray[0], capturedGauges.get(0).getValue());
        assertEquals(testIntArray[1], capturedGauges.get(1).getValue());
        assertEquals(testIntArray[2], capturedGauges.get(2).getValue());
        assertEquals(testIntArray[0], capturedGauges.get(3).getValue());
        assertEquals(testIntArray[1], capturedGauges.get(4).getValue());
        assertEquals(testIntArray[2], capturedGauges.get(5).getValue());
    }

    @Test
    public void nestedArrayStringTest() throws Exception {
        standardSetUp();
        JsonObject jsonWStringArray = new JsonObject();
        jsonWStringArray.add("array", gson.toJsonTree(testStringArray));
        jsonTestValue.add("nesting", jsonWStringArray);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(6)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(2), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(3), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(4), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(5), "aws.dbaas.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(testStringArray[0], capturedGauges.get(0).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(1).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(2).getValue());
        assertEquals(testStringArray[0], capturedGauges.get(3).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(4).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(5).getValue());
    }

    @Test
    public void nestedArrayInObjectTest() throws Exception {
        standardSetUp();
        JsonObject jsonWStringArray = new JsonObject();
        JsonObject jsonNestedArrWString = new JsonObject();
        jsonWStringArray.add("array", gson.toJsonTree(testStringArray));
        jsonNestedArrWString.add("nesting", jsonWStringArray);
        jsonTestValue.add("topLevel", jsonNestedArrWString);
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(6)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(2), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(3), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(4), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(5), "aws.dbaas.topLevel.nesting.array[Environment:test,InstanceID:instanceID]");
        assertEquals(testStringArray[0], capturedGauges.get(0).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(1).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(2).getValue());
        assertEquals(testStringArray[0], capturedGauges.get(3).getValue());
        assertEquals(testStringArray[1], capturedGauges.get(4).getValue());
        assertEquals(testStringArray[2], capturedGauges.get(5).getValue());
    }

    @Test
    public void nestedObjectInArrayTest() throws Exception {
        standardSetUp();
        JsonObject jsonWInt = new JsonObject();
        jsonWInt.addProperty("number", testValue);
        JsonObject[] testObjectArray = {jsonWInt};
        jsonTestValue.add("topLevel", gson.toJsonTree(testObjectArray));
        when(logEvent.getMessage()).thenReturn(jsonTestValue.toString());
        enhancedMetricsProcessor.runOneIteration();
        verify(metricRegistry, times(2)).register(metricNameCaptor.capture(), gaugeArgumentCaptor.capture());
        List<String> capturedNames = metricNameCaptor.getAllValues();
        List<Gauge> capturedGauges = gaugeArgumentCaptor.getAllValues();
        assertEquals(capturedNames.get(0), "aws.dbaas.topLevel.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedNames.get(1), "aws.dbaas.topLevel.number[Environment:test,InstanceID:instanceID]");
        assertEquals(capturedGauges.get(0).getValue(), testValue);
        assertEquals(capturedGauges.get(1).getValue(), testValue);
    }
}
