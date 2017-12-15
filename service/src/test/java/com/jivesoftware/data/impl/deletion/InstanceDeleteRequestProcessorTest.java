package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.QueueSendingException;
import com.jivesoftware.data.impl.DatabaseDAO;
import com.jivesoftware.data.impl.PasswordManager;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InstanceDeleteRequestProcessorTest {

    private InstanceDeleteRequestProcessor instanceDeleteRequestProcessor;

    @Mock
    private AmazonSQS sqs;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DeletionStepProcessorFactory deletionStepProcessorFactory;

    @Mock
    private PasswordManager passwordManager;

    @Mock
    private Environment environment;

    @Mock
    private LifecycleEnvironment lifecycleEnvironment;

    @Mock
    private GetQueueUrlResult getQueueUrlResult;

    @Mock
    private ReceiveMessageResult receiveMessageResult;

    @Mock
    private Message message;

    @Mock
    private DeleteCommandProcessor deleteCommandProcessor;

    @Mock
    private DBaaSConfiguration.QueueConfig deleteQueue;

    @Before
    public void setUp() {
        when(dBaaSConfiguration.getDeletionQueue()).thenReturn(deleteQueue);
        when(deleteQueue.getQueueName()).thenReturn("deleteQueueName");
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(sqs.getQueueUrl("deleteQueueName")).thenReturn(getQueueUrlResult);
        when(getQueueUrlResult.getQueueUrl()).thenReturn("queueUrl");
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);
        instanceDeleteRequestProcessor = new InstanceDeleteRequestProcessor(sqs,
                dBaaSConfiguration, databaseDAO, deletionStepProcessorFactory, passwordManager);
    }

    @Test
    public void createNewQueueTest() {

        doThrow(QueueDoesNotExistException.class).when(sqs).getQueueUrl("deleteQueueName");
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        instanceDeleteRequestProcessor = new InstanceDeleteRequestProcessor(sqs,
                dBaaSConfiguration, databaseDAO, deletionStepProcessorFactory, passwordManager);
        ArgumentCaptor<CreateQueueRequest> createRequestCaptor =
                ArgumentCaptor.forClass(CreateQueueRequest.class);
        verify(sqs).createQueue(createRequestCaptor.capture());
        assertEquals(createRequestCaptor.getValue().getQueueName(), "deleteQueueName");
    }

    @Test(expected = QueueSendingException.class)
    public void requestSoftDeleteSQSExceptionTest() {
        setUp();
        instanceDeleteRequestProcessor.requestSoftDelete("databaseId");

        doThrow(Exception.class).when(sqs).sendMessage(any());

        instanceDeleteRequestProcessor.requestSoftDelete("databaseId");
    }

    @Test
    public void requestSoftDeleteSuccessTest() {
        setUp();

        when(passwordManager.generatePassword()).thenReturn("password");

        instanceDeleteRequestProcessor.requestSoftDelete("databaseId");

        ArgumentCaptor<DatabaseStatus> statusCaptor = ArgumentCaptor.forClass(DatabaseStatus.class);
        verify(databaseDAO).updateStatus(statusCaptor.capture());
        assertEquals(statusCaptor.getValue().getDatabaseId(), "databaseId");
        assertEquals(statusCaptor.getValue().getStatus(), DatabaseStatus.Status.DELETING);
        assertNull(statusCaptor.getValue().getMessage());

        ArgumentCaptor<SendMessageRequest> messageRequestCaptor =
                ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqs).sendMessage(messageRequestCaptor.capture());
        assertEquals(messageRequestCaptor.getValue().getMessageBody(), "databaseId");
        assertEquals(messageRequestCaptor.getValue().getQueueUrl(), "queueUrl");
        assertEquals(messageRequestCaptor.getValue().getMessageAttributes()
                        .get("step").getStringValue(), DeletionStep.PREPARING.toString());
        assertEquals(messageRequestCaptor.getValue().getMessageAttributes()
                        .get("password").getStringValue(), "password");
    }

    @Test
    public void noMessagesReceivedTest() throws Exception{
        setUp();
        when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.of());
        instanceDeleteRequestProcessor.runOneIteration();
        verify(deletionStepProcessorFactory, times(0)).getStep(any());
    }

    @Test
    public void stepExceptionTest() throws Exception{
        setUp();
        when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.of(message));
        when(message.getBody()).thenReturn("messageBody");
        MessageAttributeValue stepValue = new MessageAttributeValue()
                .withStringValue("badStep");
        MessageAttributeValue passwordValue = new MessageAttributeValue()
                .withStringValue("password");
        ImmutableMap<String, MessageAttributeValue> messageMap =
                ImmutableMap.<String, MessageAttributeValue>builder()
                .put("step", stepValue)
                .put("password", passwordValue)
                .build();
        when(message.getMessageAttributes()).thenReturn(messageMap);
        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        instanceDeleteRequestProcessor.runOneIteration();
        verify(deletionStepProcessorFactory, times(0)).getStep(any());
        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());
        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.ERROR);
    }

    @Test
    public void nextStepRequiredTest() throws Exception{
        setUp();
        when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.of(message));
        when(message.getBody()).thenReturn("databaseId");
        MessageAttributeValue stepValue = new MessageAttributeValue()
                .withStringValue("PREPARING");
        MessageAttributeValue passwordValue = new MessageAttributeValue()
                .withStringValue("password");
        ImmutableMap<String, MessageAttributeValue> messageMap =
                ImmutableMap.<String, MessageAttributeValue>builder()
                        .put("step", stepValue)
                        .put("password", passwordValue)
                        .build();
        when(message.getMessageAttributes()).thenReturn(messageMap);
        when(deletionStepProcessorFactory.getStep(DeletionStep.PREPARING))
                .thenReturn(deleteCommandProcessor);
        when(deleteCommandProcessor.process("databaseId", "password"))
                .thenReturn(Optional.of(DeletionStep.DELETING));
        instanceDeleteRequestProcessor.runOneIteration();
        verify(sqs).sendMessage(any());
        verify(sqs).deleteMessage(any());
    }

    @Test
    public void lastStepCompletedTest() throws Exception{
        setUp();
        when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.of(message));
        when(message.getBody()).thenReturn("databaseId");
        MessageAttributeValue stepValue = new MessageAttributeValue()
                .withStringValue("PREPARING");
        MessageAttributeValue passwordValue = new MessageAttributeValue()
                .withStringValue("password");
        ImmutableMap<String, MessageAttributeValue> messageMap =
                ImmutableMap.<String, MessageAttributeValue>builder()
                        .put("step", stepValue)
                        .put("password", passwordValue)
                        .build();
        when(message.getMessageAttributes()).thenReturn(messageMap);
        when(deletionStepProcessorFactory.getStep(DeletionStep.PREPARING))
                .thenReturn(deleteCommandProcessor);
        when(deleteCommandProcessor.process("databaseId", "password"))
                .thenReturn(Optional.empty());
        instanceDeleteRequestProcessor.runOneIteration();
        verify(databaseDAO).updateStatus(any());
        verify(sqs, times(0)).sendMessage(any());
        verify(sqs).deleteMessage(any());
    }


}
