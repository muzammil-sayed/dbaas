package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.TokenNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PermissionManagerBadTokenFileTest {

    private PermissionManager permissionManager;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Before
    public void setUp() {
        when(dBaaSConfiguration.getTokenFile()).thenReturn("garbage/uri");
    }

    @Test(expected = TokenNotFoundException.class)
    public void tokenFileNotFound() {
        permissionManager = new PermissionManager(dBaaSConfiguration);
    }
}
