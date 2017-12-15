package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.TokenNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PermissionManagerTest {

    private PermissionManager permissionManager;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Before
    public void regularSetUp() {
        when(dBaaSConfiguration.getTokenFile()).thenReturn("src/test/resources/token-test");
        permissionManager = new PermissionManager(dBaaSConfiguration);
    }

    @Test
    public void getAuthObjectTest() {
        String permissionToken = permissionManager.getAuthObject();
        assertEquals(permissionToken, "token");
    }

    @Test
    public void isAllowedTest() {
        assertTrue(permissionManager.isAllowed("token"));
    }

    @Test
    public void isNotAllowedTest() {
        assertFalse(permissionManager.isAllowed("liesAndDeceit"));
    }
}
