package com.jivesoftware.data.impl.deletion;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.QueueSendingException;
import com.jivesoftware.data.impl.DatabaseDAO;
import com.jivesoftware.data.impl.PasswordManager;
import com.jivesoftware.data.resources.entities.DatabaseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class InstanceDeleteRequestProcessor extends AbstractScheduledService {

    private final static Logger logger = LoggerFactory.getLogger(InstanceDeleteRequestProcessor.class);

    private static final String STEP_REQUEST_KEY = "step";
    private static final String PASSWORD = "password";

    private final AmazonSQS sqs;
    private final DatabaseDAO databaseDAO;
    private final DeletionStepProcessorFactory deletionStepProcessorFactory;
    private final PasswordManager passwordManager;
    private final DBaaSConfiguration.QueueConfig deleteQueue;

    @Inject
    public InstanceDeleteRequestProcessor(AmazonSQS sqs,
                                          DBaaSConfiguration dBaaSConfiguration,
                                          DatabaseDAO databaseDAO,
                                          DeletionStepProcessorFactory deletionStepProcessorFactory,
                                          PasswordManager passwordManager) {

        this.sqs = sqs;
        this.databaseDAO = databaseDAO;
        this.deletionStepProcessorFactory = deletionStepProcessorFactory;
        this.passwordManager = passwordManager;
        this.deleteQueue = dBaaSConfiguration.getDeletionQueue();

        try {
            this.sqs.getQueueUrl(deleteQueue.getQueueName());
        } catch (QueueDoesNotExistException e) {
            try {
                CreateQueueRequest createQueueRequest = new CreateQueueRequest(deleteQueue.getQueueName());
                this.sqs.createQueue(createQueueRequest);
            } catch (QueueNameExistsException r) {

            }
        }
    }

    public void requestSoftDelete(String databaseId) {

        DatabaseStatus databaseStatus = new DatabaseStatus(DatabaseStatus.Status.DELETING,
                null, databaseId);
        databaseDAO.updateStatus(databaseStatus);

        requestStep(databaseId, DeletionStep.PREPARING, passwordManager.generatePassword());
    }

    @Override
    protected void runOneIteration() throws Exception{

        String myQueueUrl = sqs.getQueueUrl(deleteQueue.getQueueName()).getQueueUrl();
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                        .withMessageAttributeNames(
                                STEP_REQUEST_KEY,
                                PASSWORD);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            if (message.getMessageAttributes().isEmpty()) {
                continue;
            }
            String databaseId = message.getBody();
            String password;
            try {
                DeletionStep step = DeletionStep.valueOf(message.getMessageAttributes()
                        .get(STEP_REQUEST_KEY).getStringValue());
                password = message.getMessageAttributes().get(PASSWORD).getStringValue();

                logger.debug(String.format("Step: %s DatabaseID: %s", step.name(), databaseId));

                Optional<DeletionStep> nextStep = deletionStepProcessorFactory.getStep(step)
                        .process(databaseId, password);

                if (nextStep.isPresent()) {
                    requestStep(databaseId, nextStep.get(), password);
                }
                else {
                    databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.DELETED,
                            null, databaseId));
                }
            } catch (Exception e) {
                String errorMessage = String.format("Error processing deletion step: %s",
                        e.getMessage());
                databaseDAO.updateStatus(new DatabaseStatus(DatabaseStatus.Status.ERROR,
                        errorMessage, databaseId));
                logger.error(errorMessage);
            }

            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
        }
    }

    public void requestStep(String databaseId, DeletionStep deletionStep, String password) {

        try {
            sqs.sendMessage(new SendMessageRequest(sqs.getQueueUrl(
                    deleteQueue.getQueueName())
                    .getQueueUrl(),
                    databaseId)
                    .addMessageAttributesEntry(STEP_REQUEST_KEY,
                            new MessageAttributeValue().withDataType("String")
                                    .withStringValue(deletionStep.name()))
                    .addMessageAttributesEntry(PASSWORD,
                            new MessageAttributeValue().withDataType("String")
                                    .withStringValue(password)));
        }
        catch (Exception e) {
            logger.error(String.format("Step %s had an exception for %s",
                    deletionStep.toString(), databaseId));
            throw new QueueSendingException(e.getMessage());
        }
    }


    @Override
    protected AbstractScheduledService.Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedRateSchedule(0,
                deleteQueue.getQueuePollFrequencySeconds(), TimeUnit.SECONDS);
    }
}
