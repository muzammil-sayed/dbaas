package com.jivesoftware.data.resources;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.jivesoftware.data.impl.ManagedPeriodicTask;
import io.dropwizard.setup.Environment;
import ru.vyarus.dropwizard.guice.module.installer.FeatureInstaller;
import ru.vyarus.dropwizard.guice.module.installer.install.InstanceInstaller;
import ru.vyarus.dropwizard.guice.module.installer.util.FeatureUtils;

public class ScheduledServiceInstaller implements FeatureInstaller<AbstractScheduledService>,
        InstanceInstaller<AbstractScheduledService>
{
    @Override
    public boolean matches(final Class<?> type) {
        return FeatureUtils.is(type, AbstractScheduledService.class);
    }

    @Override
    public void install(final Environment environment, AbstractScheduledService scheduledService) {
        environment.lifecycle().manage(new ManagedPeriodicTask(scheduledService));
    }

    @Override
    public void report(){

    }
}