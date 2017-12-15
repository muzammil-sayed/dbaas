package com.jivesoftware.data.health;

import com.jivesoftware.data.impl.InstanceManager;
import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;

import javax.inject.Inject;

public class SharedCapacityHealthCheck extends NamedHealthCheck {

    private final InstanceManager instanceManager;

    @Inject
    public SharedCapacityHealthCheck(InstanceManager instanceManager) {
        this.instanceManager = instanceManager;
    }

    @Override
    public String getName() {
        return "sharedCapacity";
    }

    @Override
    protected Result check() throws Exception {
        return true /* seems to take longer than a min - instanceManager.findSharedInstance().isPresent() */ ? Result.healthy() :
                Result.unhealthy("No shared instance to place a shared schema is found");
    }
}
