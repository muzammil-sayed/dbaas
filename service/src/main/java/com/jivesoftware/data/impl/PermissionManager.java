package com.jivesoftware.data.impl;

import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.TokenNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class PermissionManager {

    private final static Logger logger = LoggerFactory.getLogger(PermissionManager.class);

    private final String permissionToken;

    @Inject
    public PermissionManager(DBaaSConfiguration dBaaSConfiguration) {
        this.permissionToken = load(dBaaSConfiguration.getTokenFile());
    }

    public String getAuthObject(){
        return permissionToken;
    }

    public boolean isAllowed(String token) {
        return permissionToken.equals(token);
    }

    private String load(String tokenLocation) {

        try {
            List<String> tokenContent = Files.readAllLines(Paths.get(tokenLocation),
                    Charset.forName("UTF-8"));
            if(!tokenContent.isEmpty()) {
                return tokenContent.get(0);
            }
        } catch (Exception e) {
            logger.error(String.format("Error reading token auth file %s",
                    tokenLocation));
            throw new TokenNotFoundException(e.getMessage());
        }
        logger.error(String.format("Empty token file at %s", tokenLocation));
        throw new TokenNotFoundException(String.format("Token file empty at %s", tokenLocation));
    }
}
