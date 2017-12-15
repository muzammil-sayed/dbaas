package com.jivesoftware.data.impl;

import com.google.common.collect.ImmutableList;
import com.jivesoftware.data.DBaaSConfiguration;
import org.junit.Before;
import org.mockito.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultInstanceLoaderTest {

    private DefaultInstanceLoader defaultInstanceLoader;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DBaaSConfiguration.InstanceTemplate instanceTemplate;

    private List<String> instanceNames;

    @Before
    public void setup() {
        instanceNames = ImmutableList.of("default1", "default2");
        when(dBaaSConfiguration.getDefaultInstances()).thenReturn(instanceNames);
        when(dBaaSConfiguration.getInstanceTemplate()).thenReturn(instanceTemplate);
        when(instanceTemplate.getAllocatedStorage()).thenReturn(10);
        when(instanceTemplate.getDbInstanceClass()).thenReturn("db.r3.large");
        when(dBaaSConfiguration.getDefaultDBName()).thenReturn("dbName");
        defaultInstanceLoader = new DefaultInstanceLoader(dBaaSConfiguration);
    }

    @Test
    public void getInstanceListTest() {

        List<DefaultInstanceLoader.DefaultInstance> defaultInstanceList =
                defaultInstanceLoader.getInstanceList();

        assertEquals(defaultInstanceList.size(), 2);
        assertEquals(defaultInstanceList.get(0).getIdentifier(), "default1");
        assertEquals(defaultInstanceList.get(0).getAllocatedStorage().intValue(), 10);
        assertEquals(defaultInstanceList.get(0).getDbInstanceClass(), "db.r3.large");
        assertEquals(defaultInstanceList.get(0).getDbName(), "dbName");
        assertFalse(defaultInstanceList.get(0).getIdentifier() == defaultInstanceList.get(1).getIdentifier());

    }

}
