package com.jivesoftware.data.impl;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.EncryptionException;
import com.jivesoftware.data.exceptions.QueueSendingException;
import com.jivesoftware.data.exceptions.SchemaOperationException;
import com.jivesoftware.data.impl.message_serializer.CreationMessageSerializer;
import com.jivesoftware.data.impl.message_serializer.EncryptionManager;
import com.jivesoftware.data.impl.message_serializer.EncryptionObject;
import com.jivesoftware.data.resources.entities.DatabaseCreateResponse;
import com.jivesoftware.data.resources.entities.DatabaseCreationRequest;
import com.jivesoftware.data.resources.entities.DatabaseStatus;

import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseCreateRequestProcessorTest {

    private DatabaseCreateRequestProcessor databaseCreateRequestProcessor;

    @Mock
    private AmazonSQS sqs;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DatabaseDAO databaseDAO;

    @Mock
    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private PasswordManager passwordManager;

    @Mock
    private StepProcessorFactory stepProcessorFactory;

    @Mock
    private DatabaseCreationRequest databaseCreationRequest;

    @Mock
    private LifecycleEnvironment lifecycleEnvironment;

    @Mock
    private GetQueueUrlResult queueUrlResult;

    @Mock
    private ReceiveMessageResult receiveMessageResult;

    @Mock
    private Message message;

    @Mock
    private InstanceCreationProcessor instanceCreationProcessor;

    @Mock
    private SchemaCreationProcessor schemaCreationProcessor;

    @Mock
    private CloneProcessor cloneProcessor;

    @Mock
    private ReadyProcessor readyProcessor;

    @Mock
    private DBaaSConfiguration.QueueConfig createQueue;

    @Mock
    private EncryptionManager encryptionManager;

    @Mock
    private EncryptionObject encryptionObject;

    @Mock
    private CreationRequestMessage creationRequestMessage;

    @Mock
    private CreationMessageSerializer creationMessageSerializer;

    private List<Message> messageList;

    byte[] ivArray = new byte[10];
    byte[] messageArray = new byte[10];
    ByteBuffer ivBuffer = ByteBuffer.wrap(ivArray);
    ByteBuffer messageBuffer = ByteBuffer.wrap(messageArray);

    MessageAttributeValue version = new MessageAttributeValue().withStringValue("v1");
    MessageAttributeValue ivAttribute = new MessageAttributeValue().withBinaryValue(ivBuffer);
    MessageAttributeValue messageAttribute = new MessageAttributeValue().withBinaryValue(messageBuffer);

    String stringBody;

    Map<String, MessageAttributeValue> responseMap;

    ArgumentCaptor<SendMessageRequest> messageRequestArgumentCaptor =
            ArgumentCaptor.forClass(SendMessageRequest.class);

    @Before
    public void setUp(){

        when(dBaaSConfiguration.getCreationQueue()).thenReturn(createQueue);
        when(createQueue.getQueueName()).thenReturn("queueName");
        when(sqs.getQueueUrl("queueName")).thenReturn(queueUrlResult);
        databaseCreateRequestProcessor = defaultTestConstructor(sqs);

        messageList = ImmutableList.of(message);

        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);
        when(receiveMessageResult.getMessages()).thenReturn(messageList);

        when(databaseCreationRequest.getCategory()).thenReturn("category");
        when(databaseIDHelper.generateDatabaseId("category")).thenReturn("databaseId");
        when(passwordManager.generatePassword()).thenReturn("password");

        when(encryptionObject.getIV()).thenReturn(ivArray);
        when(encryptionObject.getEncryptedMessage()).thenReturn(messageArray);

        stringBody = new String(messageArray, Charset.forName("UTF-8"));
        when(message.getBody()).thenReturn("dummyBody");

        when(creationRequestMessage.getDatabaseCreationRequest()).thenReturn(databaseCreationRequest);
        when(creationRequestMessage.getDatabaseId()).thenReturn("databaseId");
        when(creationRequestMessage.getPassword()).thenReturn("password");

        when(creationMessageSerializer.deserialize("decryptedJSONObject", CreationRequestMessage.class))
                .thenReturn(creationRequestMessage);

    }

    private DatabaseCreateRequestProcessor defaultTestConstructor(AmazonSQS sqs){

        return new DatabaseCreateRequestProcessor(
                sqs, dBaaSConfiguration, databaseDAO, databaseIDHelper,
                passwordManager, stepProcessorFactory, encryptionManager,
                creationMessageSerializer);
    }

    @Test
    public void createQueueTest(){

        doThrow(QueueDoesNotExistException.class).when(sqs).getQueueUrl("queueName");
        defaultTestConstructor(this.sqs);

        verify(sqs).createQueue(any(CreateQueueRequest.class));
    }

    @Test
    public void requestDatabaseInstanceCreationTest() {

        when(databaseCreationRequest.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.DEDICATED);
        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);

        DatabaseCreateResponse createResponseReturn =
                databaseCreateRequestProcessor.requestDatabaseCreation(databaseCreationRequest);

        verify(sqs).sendMessage(messageRequestArgumentCaptor.capture());

        Map<String, MessageAttributeValue> argumentMap = messageRequestArgumentCaptor.getValue().getMessageAttributes();

        assertEquals(argumentMap.get("version").getStringValue(), "v1");
        assertTrue(argumentMap.containsKey("createRequest"));
        assertTrue(argumentMap.containsKey("iv"));

        assertEquals(createResponseReturn.getDatabaseId(), "databaseId");
        assertEquals(createResponseReturn.getPassword(), "password");

    }

    @Test(expected = QueueSendingException.class)
    public void requestDatabaseInstanceCreationEncryptionExceptionTest() {

        when(databaseCreationRequest.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.DEDICATED);
        doThrow(EncryptionException.class).when(encryptionManager).encrypt(any());

        databaseCreateRequestProcessor.requestDatabaseCreation(databaseCreationRequest);
    }

    @Test(expected = QueueSendingException.class)
    public void requestDatabaseInstanceCreationMessageExceptionTest() {

        when(databaseCreationRequest.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.DEDICATED);
        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);;

        doThrow(InvalidMessageContentsException.class).when(sqs).sendMessage(any());

        DatabaseCreateResponse createResponseReturn =
                databaseCreateRequestProcessor.requestDatabaseCreation(databaseCreationRequest);

        assertEquals(createResponseReturn.getDatabaseId(), "databaseId");
        assertEquals(createResponseReturn.getPassword(), "password");

        when(queueUrlResult.getQueueUrl()).thenReturn("url");
    }



    @Test
    public void requestDatabaseSchemaCreationTest() {

        when(databaseCreationRequest.getTenancyType()).thenReturn(
                DatabaseCreationRequest.TenancyType.SHARED);
        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);

        DatabaseCreateResponse createResponseReturn =
                databaseCreateRequestProcessor.requestDatabaseCreation(databaseCreationRequest);

        verify(sqs).sendMessage(messageRequestArgumentCaptor.capture());

        Map<String, MessageAttributeValue> argumentMap = messageRequestArgumentCaptor.getValue().getMessageAttributes();

        assertEquals(argumentMap.get("version").getStringValue(), "v1");
        assertTrue(argumentMap.containsKey("createRequest"));
        assertTrue(argumentMap.containsKey("iv"));

        assertEquals(createResponseReturn.getDatabaseId(), "databaseId");
        assertEquals(createResponseReturn.getPassword(), "password");

    }

    @Test
    public void instanceCreateStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute, "createRequest",
                messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        when(encryptionManager.decrypt(any())).thenReturn("decryptedJSONObject");

        when(creationRequestMessage.getCreationStep()).thenReturn(CreationStep.INSTANCE);

        when(stepProcessorFactory.getStep(CreationStep.INSTANCE))
                .thenReturn(instanceCreationProcessor);
        when(instanceCreationProcessor.process("databaseId", "password", databaseCreationRequest))
                .thenReturn(Optional.of(CreationStep.INSTANCE_READY));

        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);

        databaseCreateRequestProcessor.runOneIteration();

        verify(sqs).sendMessage(messageRequestArgumentCaptor.capture());

        Map<String, MessageAttributeValue> argumentMap = messageRequestArgumentCaptor.getValue().getMessageAttributes();

        assertEquals(argumentMap.get("version").getStringValue(), "v1");
        assertTrue(argumentMap.containsKey("createRequest"));
        assertTrue(argumentMap.containsKey("iv"));

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());
    }

    @Test
    public void instanceCreateWrongVersionStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        MessageAttributeValue versionZero = new MessageAttributeValue().withStringValue("v0");

        responseMap = ImmutableMap.of("version", versionZero);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        databaseCreateRequestProcessor.runOneIteration();

        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());
        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.ERROR);

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());
    }

    @Test
    public void instanceCreateNullVersionStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("iv", ivAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        databaseCreateRequestProcessor.runOneIteration();

        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());
        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.ERROR);

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());
    }

    @Test
    public void instanceCreateEmptyMessageStepTest() throws Exception {
        when(queueUrlResult.getQueueUrl()).thenReturn("url");
        responseMap = ImmutableMap.of();

        when(message.getMessageAttributes()).thenReturn(responseMap);

        databaseCreateRequestProcessor.runOneIteration();

        verify(databaseDAO, times(0)).updateStatus(any());

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs, times(0)).deleteMessage(any());
    }

    @Test
    public void schemaCreateStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute,
                "createRequest", messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        when(encryptionManager.decrypt(any())).thenReturn("decryptedJSONObject");

        when(creationRequestMessage.getCreationStep()).thenReturn(CreationStep.SCHEMA);

        when(stepProcessorFactory.getStep(CreationStep.SCHEMA))
                .thenReturn(schemaCreationProcessor);
        when(schemaCreationProcessor.process("databaseId", "password", databaseCreationRequest))
                .thenReturn(Optional.of(CreationStep.CLONE));

        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);

        databaseCreateRequestProcessor.runOneIteration();

        verify(sqs).sendMessage(messageRequestArgumentCaptor.capture());

        Map<String, MessageAttributeValue> argumentMap = messageRequestArgumentCaptor.getValue().getMessageAttributes();

        assertEquals(argumentMap.get("version").getStringValue(), "v1");
        assertTrue(argumentMap.containsKey("createRequest"));
        assertTrue(argumentMap.containsKey("iv"));

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());

    }

    @Test
    public void cloneCreateStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute,
                "createRequest", messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        when(encryptionManager.decrypt(any())).thenReturn("decryptedJSONObject");

        when(creationRequestMessage.getCreationStep()).thenReturn(CreationStep.CLONE);

        when(stepProcessorFactory.getStep(CreationStep.CLONE)).thenReturn(cloneProcessor);
        when(cloneProcessor.process("databaseId", "password", databaseCreationRequest))
                .thenReturn(Optional.empty());

        databaseCreateRequestProcessor.runOneIteration();

        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        verify(sqs, never()).sendMessage(any());
        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());

        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.READY);

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());
    }

    @Test
    public void readyCreateStepTest() throws Exception{
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute,
                "createRequest", messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        when(encryptionManager.decrypt(any())).thenReturn("decryptedJSONObject");

        when(creationRequestMessage.getCreationStep()).thenReturn(CreationStep.INSTANCE_READY);

        when(stepProcessorFactory.getStep(CreationStep.INSTANCE_READY))
                .thenReturn(readyProcessor);
        when(readyProcessor.process("databaseId", "password", databaseCreationRequest))
                .thenReturn(Optional.of(CreationStep.SCHEMA));

        when(encryptionManager.encrypt(any())).thenReturn(encryptionObject);

        databaseCreateRequestProcessor.runOneIteration();

        verify(sqs).sendMessage(messageRequestArgumentCaptor.capture());

        Map<String, MessageAttributeValue> argumentMap = messageRequestArgumentCaptor.getValue().getMessageAttributes();

        assertEquals(argumentMap.get("version").getStringValue(), "v1");
        assertTrue(argumentMap.containsKey("createRequest"));
        assertTrue(argumentMap.containsKey("iv"));

        when(message.getReceiptHandle()).thenReturn("receiptHandle");

        verify(sqs).deleteMessage(any());
    }

    @Test
    public void stepErrorTest() throws Exception {
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute,
                "createRequest", messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        when(encryptionManager.decrypt(any())).thenReturn("decryptedJSONObject");

        when(creationRequestMessage.getCreationStep()).thenReturn(CreationStep.INSTANCE_READY);

        when(stepProcessorFactory.getStep(CreationStep.INSTANCE_READY))
                .thenReturn(readyProcessor);
        doThrow(SchemaOperationException.class).when(readyProcessor)
                .process("databaseId", "password", databaseCreationRequest);

        databaseCreateRequestProcessor.runOneIteration();

        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());
        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.ERROR);
    }

    @Test
    public void stepIterationEncryptionException() throws Exception {
        when(queueUrlResult.getQueueUrl()).thenReturn("url");

        responseMap = ImmutableMap.of("version", version, "iv", ivAttribute,
                "createRequest", messageAttribute);

        when(message.getMessageAttributes()).thenReturn(responseMap);

        doThrow(EncryptionException.class).when(encryptionManager).decrypt(any());

        databaseCreateRequestProcessor.runOneIteration();

        ArgumentCaptor<DatabaseStatus> statusArgumentCaptor =
                ArgumentCaptor.forClass(DatabaseStatus.class);

        verify(databaseDAO).updateStatus(statusArgumentCaptor.capture());
        assertEquals(statusArgumentCaptor.getValue().getStatus(), DatabaseStatus.Status.ERROR);
    }

}
