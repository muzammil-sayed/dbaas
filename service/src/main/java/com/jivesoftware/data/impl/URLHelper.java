package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public class URLHelper {

    private final static Logger logger = LoggerFactory.getLogger(URLHelper.class);

    private static final String RESOURCE = String.format("/%s/%s/", "v1", "databases");
    private static final String STATUS = String.format("/%s", "status");

    private final DBaaSConfiguration.DomainDetails domainDetails;

    @Inject
    public URLHelper(DBaaSConfiguration dBaaSConfiguration) {
        this.domainDetails = dBaaSConfiguration.getDomainDetails();
    }

    public Optional<URI> buildURL(String databaseId) {
        String url = domainDetails.getProtocol() + domainDetails.getDomainName() +
                RESOURCE + databaseId + STATUS;
        try {
            return Optional.of(new URI(url));
        } catch (URISyntaxException se ) {
            logger.error(String.format("URL construction failed: %s", se.getMessage()));
            return Optional.empty();
        }

    }
}
