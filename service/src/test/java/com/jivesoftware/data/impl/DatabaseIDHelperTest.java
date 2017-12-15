package com.jivesoftware.data.impl;

import com.amazonaws.services.rds.model.DBInstance;
import com.jivesoftware.data.DBaaSConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseIDHelperTest {

    private DatabaseIDHelper databaseIDHelper;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DBInstance dbInstance;


    public void shortSetUp() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix()).thenReturn("dbaas-test");

        databaseIDHelper = new DatabaseIDHelper(dBaaSConfiguration);
    }

    public void longSetUp() {
        when(dBaaSConfiguration.getInstanceIdentifierPrefix())
                .thenReturn("data-dbaas-aws-us-west-2-brew-prod");

        databaseIDHelper = new DatabaseIDHelper(dBaaSConfiguration);
    }

    @Test
     public void testLegalDatabaseId() {
        shortSetUp();
        assertFalse(databaseIDHelper.generateDatabaseId("test-dh").contains("-"));
    }

    @Test
    public void testLegalDBInstanceId() {
        shortSetUp();
        assertEquals(databaseIDHelper.getDBInstanceId("the_test"), "dbaas-test-the-test");
    }

    @Test
    public void testdatabaseInstanceIdGet(){
        shortSetUp();
        when(dbInstance.getDBInstanceIdentifier()).thenReturn("dbaas-test-the-test");
        assertEquals(databaseIDHelper.getDatabaseInstanceId(dbInstance), "the-test");
    }

    @Test
    public void testTooLongCategory() {
        longSetUp();
        String category = "jcx-app-productionreallylongname";
        String generatedID = databaseIDHelper.generateDatabaseId(category);
        String instanceID = databaseIDHelper.getDBInstanceId(generatedID);
        assertTrue(instanceID.length() < 64);
    }

    @Test
    public void testPrettyLongCategory() {
        longSetUp();
        String category = "jcx-app-productionreal";
        String generatedID = databaseIDHelper.generateDatabaseId(category);
        String instanceID = databaseIDHelper.getDBInstanceId(generatedID);
        assertTrue(instanceID.length() < 64);
    }

    @Test
    public void testKindaShortCategory() {
        longSetUp();
        String category = "jcx-app-production";
        String generatedID = databaseIDHelper.generateDatabaseId(category);
        String instanceID = databaseIDHelper.getDBInstanceId(generatedID);
        assertTrue(instanceID.length() < 64);
        assertTrue(instanceID.contains(category));
    }

    @Test
    public void testReallyShortCategory() {
        shortSetUp();
        String category = "jcx-app-production";
        String generatedID = databaseIDHelper.generateDatabaseId(category);
        String instanceID = databaseIDHelper.getDBInstanceId(generatedID);
        assertTrue(instanceID.length() < 64);
        assertTrue(instanceID.contains(category));
        String suffix = instanceID.substring(instanceID.length() - 9);
        assertEquals(suffix.substring(0,1), "-");
    }

}