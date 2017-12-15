package com.jivesoftware.data.impl;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.EncryptionException;
import com.jivesoftware.data.exceptions.QueueSendingException;
import com.jivesoftware.data.impl.message_serializer.EncryptionManager;
import com.jivesoftware.data.impl.message_serializer.EncryptionObject;
import com.jivesoftware.data.impl.message_serializer.CreationMessageSerializer;
import com.jivesoftware.data.resources.entities.*;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DatabaseCreateRequestProcessor extends AbstractScheduledService {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseCreateRequestProcessor.class);

    private static final String RECEIVE_ALL_MESSAGE_ATTRIBUTES = "All";
    private static final String INITIALIZATION_VECTOR = "iv";
    private static final String CURRENT_VERSION = "v1";
    private static final String VERSION = "version";
    private static final String CREATEREQUEST = "createRequest";
    private static final String MESSAGEBODY = "blah blah I'm a body that can be checksummed with md5";
    private static final String CREATEMESSAGE = "%s is still being created";

    private final AmazonSQS sqs;
    private final DatabaseDAO databaseDAO;
    private final DatabaseIDHelper databaseIDHelper;
    private final PasswordManager passwordManager;
    private final StepProcessorFactory stepProcessorFactory;
    private final DBaaSConfiguration.QueueConfig createQueue;
    private final EncryptionManager encryptionManager;
    private final CreationMessageSerializer messageSerializer;

    @Inject
    public DatabaseCreateRequestProcessor(AmazonSQS sqs,
                                          DBaaSConfiguration dBaaSConfiguration,
                                          DatabaseDAO databaseDAO,
                                          DatabaseIDHelper databaseIDHelper,
                                          PasswordManager passwordManager,
                                          StepProcessorFactory stepProcessorFactory,
                                          EncryptionManager encryptionManager,
                                          CreationMessageSerializer messageSerializer) {
        this.sqs = sqs;
        this.databaseDAO = databaseDAO;
        this.databaseIDHelper = databaseIDHelper;
        this.passwordManager = passwordManager;
        this.stepProcessorFactory = stepProcessorFactory;
        this.encryptionManager = encryptionManager;
        this.messageSerializer = messageSerializer;
        this.createQueue = dBaaSConfiguration.getCreationQueue();

        try {
            this.sqs.getQueueUrl(createQueue.getQueueName());
        } catch (QueueDoesNotExistException e) {
            try {
                CreateQueueRequest createQueueRequest = new CreateQueueRequest(createQueue.getQueueName());
                this.sqs.createQueue(createQueueRequest);
            } catch (QueueNameExistsException r) {

            }
        }
    }

    public DatabaseCreateResponse requestDatabaseCreation(DatabaseCreationRequest databaseCreationRequest) {
        String databaseId = databaseIDHelper.generateDatabaseId(databaseCreationRequest.getCategory());
        logger.debug(String.format("Starting creation process for %s", databaseId));

        String password = passwordManager.generatePassword();

        CreationStep creationStep =
                databaseCreationRequest.getTenancyType() == DatabaseCreationRequest.TenancyType.DEDICATED
                ? CreationStep.INSTANCE : CreationStep.SCHEMA;

        databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.CREATING,
                String.format(CREATEMESSAGE, databaseId), databaseId));

        logger.debug(String.format("Database %s added to Dynamo records", databaseId));

        requestStep(databaseId, password, creationStep, databaseCreationRequest);

        logger.debug(String.format("Creation step sent to SQS for %s", databaseId));

        return new DatabaseCreateResponse(databaseId, password);
    }

    @Override
    protected void runOneIteration() throws Exception {

        String myQueueUrl = sqs.getQueueUrl(createQueue.getQueueName()).getQueueUrl();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                .withMessageAttributeNames(
                        RECEIVE_ALL_MESSAGE_ATTRIBUTES);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getMessageAttributes().isEmpty()) {
                continue;
            }
            String databaseId = "";
            try {
                DatabaseCreationRequest databaseCreationRequest;
                String password;
                CreationStep step;
                CreationRequestMessage receivedMessage;

                MessageAttributeValue messageVersion = message.getMessageAttributes().get(VERSION);
                if(messageVersion == null){
                    throw new EncryptionException("No version was sent with your message.  This " +
                            "could be due to an upgrade while your message was in flight.");
                }
                String version = messageVersion.getStringValue();

                if ("v1".equals(version)) {
                    EncryptionObject receivedObject =
                            new EncryptionObject(message.getMessageAttributes()
                                    .get(INITIALIZATION_VECTOR).getBinaryValue().array(),
                                    message.getMessageAttributes().get(CREATEREQUEST)
                                            .getBinaryValue().array());
                    String decryptedMessage = encryptionManager.decrypt(receivedObject);
                    receivedMessage = messageSerializer.deserialize(decryptedMessage, CreationRequestMessage.class);

                    databaseCreationRequest = receivedMessage.getDatabaseCreationRequest();
                    databaseId = receivedMessage.getDatabaseId();
                    password = receivedMessage.getPassword();
                    step = receivedMessage.getCreationStep();
                }
                else {
                    throw new EncryptionException("The system's queue messaging system was upgraded " +
                            "while your message was in flight, OR an unexpected value was received " +
                            "for message version. Please request a new database.");
                }

                logger.debug(String.format("Step: %s DatabaseID: %s", step.name(), databaseId));

                Optional<CreationStep> nextStep = stepProcessorFactory.getStep(step)
                        .process(databaseId, password, databaseCreationRequest);

                if (nextStep.isPresent()) {
                    requestStep(databaseId, password, nextStep.get(), databaseCreationRequest);
                }
                else {
                    databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.READY,
                            null, databaseId));
                }
            } catch (EncryptionException ee) {
                databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.ERROR,
                        ee.getMessage(), databaseId));
                logger.error(String.format("Error decrypting received create message"));
            }
            catch (Exception e) {
                databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.ERROR,
                        e.getMessage(), databaseId));
                logger.error("Error processing creation step.", e.getMessage());
            }

            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));

        }
    }

    public void requestStep(String databaseId, String password, CreationStep creationStep,
                            DatabaseCreationRequest databaseCreationRequest) {

        CreationRequestMessage rawMessage = new CreationRequestMessage(
                databaseId, password, creationStep, databaseCreationRequest);

        EncryptionObject encryptedMessage;

        try {
            String serializedMessage = messageSerializer.serialize(rawMessage);
            encryptedMessage = encryptionManager.encrypt(serializedMessage);
        } catch (EncryptionException ee) {
            logger.error(String.format("Error encrypting create message for %s", databaseId));
            throw new QueueSendingException(ee.getMessage());
        }

        try {
            ByteBuffer encryptedIVBuffer = ByteBuffer.wrap(encryptedMessage.getIV());
            ByteBuffer encyrptedMessageBuffer = ByteBuffer.wrap(encryptedMessage.getEncryptedMessage());

            sqs.sendMessage(new SendMessageRequest(sqs.getQueueUrl(
                    createQueue.getQueueName()).getQueueUrl(), MESSAGEBODY)
                    .addMessageAttributesEntry(VERSION,
                            new MessageAttributeValue().withDataType("String")
                                    .withStringValue(CURRENT_VERSION))
                    .addMessageAttributesEntry(INITIALIZATION_VECTOR,
                            new MessageAttributeValue().withDataType("Binary")
                                    .withBinaryValue(encryptedIVBuffer))
                    .addMessageAttributesEntry(CREATEREQUEST,
                            new MessageAttributeValue().withDataType("Binary")
                                    .withBinaryValue(encyrptedMessageBuffer)));
        }
        catch (Exception e) {
            logger.error(String.format("Step %s had an exception for %s",
                    creationStep.toString(), databaseId));
            throw new QueueSendingException(e.getMessage());
        }

    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedRateSchedule(0,
                createQueue.getQueuePollFrequencySeconds(), TimeUnit.SECONDS);
    }

}
