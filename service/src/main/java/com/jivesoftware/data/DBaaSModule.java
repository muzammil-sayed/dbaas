package com.jivesoftware.data;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class DBaaSModule extends AbstractModule {

    @Override
    protected void configure() {
        requireBinding(DBaaSConfiguration.class);
        requireBinding(MetricRegistry.class);
    }


    @Provides
    @Singleton
    public AmazonDynamoDBClient provideAmazonDynamo(DBaaSConfiguration dBaaSConfiguration) {
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        amazonDynamoDBClient.setRegion(dBaaSConfiguration.getAwsRegion());
        return amazonDynamoDBClient;
    }

    @Provides
    @Singleton
    public AmazonRDSClient provideAmazonRDS(DBaaSConfiguration dBaaSConfiguration) {
        AmazonRDSClient amazonRDSClient
                = new AmazonRDSClient(new DefaultAWSCredentialsProviderChain());
        amazonRDSClient.setRegion(dBaaSConfiguration.getAwsRegion());
        return amazonRDSClient;
    }

    @Provides
    @Singleton
    public AmazonSQS provideAmazonSQS(DBaaSConfiguration dBaaSConfiguration) {
        AmazonSQS amazonSQS
                = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        amazonSQS.setRegion(dBaaSConfiguration.getAwsRegion());
        return amazonSQS;
    }

    @Provides
    @Singleton
    public AmazonCloudWatchClient provideAmazonCloudWatchClient(DBaaSConfiguration dBaaSConfiguration) {
        AmazonCloudWatchClient amazonCloudWatchClient = new AmazonCloudWatchClient(new DefaultAWSCredentialsProviderChain());
        amazonCloudWatchClient.setRegion(dBaaSConfiguration.getAwsRegion());
        return amazonCloudWatchClient;
    }

    @Provides
    @Singleton
    public AWSLogsClient provideAWSLogsClient(DBaaSConfiguration dBaaSConfiguration) {
        AWSLogsClient awsLogsClient = new AWSLogsClient(new DefaultAWSCredentialsProviderChain());
        awsLogsClient.setRegion(dBaaSConfiguration.getAwsRegion());
        return awsLogsClient;
    }
}


