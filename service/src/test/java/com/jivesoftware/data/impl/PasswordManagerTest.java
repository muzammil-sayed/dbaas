package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PasswordManagerTest {

    private PasswordManager passwordManager;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;


    @Before
    public void setUp() {
        when(dBaaSConfiguration.getPasswordFile()).thenReturn("src/test/resources/instances-test.yaml");
        passwordManager = new PasswordManager(dBaaSConfiguration);
    }

    @Test
    public void testPasswordManagerInstanceExists() {
        Optional<PasswordManager.Instance> instance = passwordManager.getInstance("test2");

        assertTrue(instance.isPresent());
        assertEquals("postgres2", instance.get().getUsername());
        assertEquals("password2", instance.get().getPassword());
    }

    @Test
    public void testPasswordManagerInstanceDoesNotExist() {
        Optional<PasswordManager.Instance> instance = passwordManager.getInstance("blah");
        assertFalse(instance.isPresent());
    }

    @Test
    public void testGeneratePassword() {
        String password = passwordManager.generatePassword();
        assertNotNull(password);
        assertTrue(password.length() > 10);
    }


}