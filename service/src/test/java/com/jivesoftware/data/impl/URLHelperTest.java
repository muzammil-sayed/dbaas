package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class URLHelperTest {

    private URLHelper urlHelper;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    @Mock
    private DBaaSConfiguration.DomainDetails domainDetails;

    @Before
    public void setUp(){
        when(dBaaSConfiguration.getDomainDetails()).thenReturn(domainDetails);
        when(domainDetails.getProtocol()).thenReturn("https://");
        when(domainDetails.getDomainName()).thenReturn("dbaas.place.com");
        urlHelper = new URLHelper(dBaaSConfiguration);
    }

    @Test
    public void urlBuilderTest() throws Exception{
        URI url = new URI("https://dbaas.place.com/v1/databases/databaseId/status");
        assertEquals(urlHelper.buildURL("databaseId"),
                Optional.of(url));
    }

}
