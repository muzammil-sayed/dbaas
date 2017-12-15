package com.jivesoftware.data.resources;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;

public class SampleResourceTest {
    private DatabaseResource resource;

    @Before
    public void before() {
        Injector injector = Guice.createInjector(new TestModule());
        //resource = injector.getInstance(DatabaseResource.class);
    }

    @Test
    public void testGetConfig() throws Exception {
        //Assert.assertNotNull(resource.get());
    }

}
