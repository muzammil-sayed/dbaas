package com.jivesoftware.data;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.jivesoftware.cloud.mako.bundle.MakoBundle;
import com.jivesoftware.data.health.SharedCapacityHealthCheck;
import com.jivesoftware.data.impl.DatabaseCreateRequestProcessor;
import com.jivesoftware.data.impl.EnhancedMetricsProcessor;
import com.jivesoftware.data.impl.deletion.InstanceDeleteRequestProcessor;
import com.jivesoftware.data.resources.DatabaseResource;
import com.jivesoftware.data.resources.RuntimeExceptionMapper;
import com.jivesoftware.data.resources.ScheduledServiceInstaller;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import ru.vyarus.dropwizard.guice.GuiceBundle;
import ru.vyarus.dropwizard.guice.module.installer.feature.TaskInstaller;
import ru.vyarus.dropwizard.guice.module.installer.feature.health.HealthCheckInstaller;
import ru.vyarus.dropwizard.guice.module.installer.feature.jersey.ResourceInstaller;


/**
 * DBaaS application.
 */
public class DBaaSApplication extends Application<DBaaSConfiguration> {

    @Override
    public void initialize(Bootstrap<DBaaSConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.addBundle(new MakoBundle());
        GuiceBundle<DBaaSConfiguration> guiceBundle = GuiceBundle.<DBaaSConfiguration>builder()
                .modules(new DBaaSModule())
                .installers(ResourceInstaller.class, TaskInstaller.class,
                        HealthCheckInstaller.class,
                        ScheduledServiceInstaller.class)
                .extensions(DatabaseResource.class,
                        SharedCapacityHealthCheck.class,
                        EnhancedMetricsProcessor.class,
                        DatabaseCreateRequestProcessor.class,
                        InstanceDeleteRequestProcessor.class)
                .build();

        bootstrap.getObjectMapper().registerModules(new ParameterNamesModule(), new Jdk8Module());
        bootstrap.addBundle(guiceBundle);
    }

    @Override
    public void run(DBaaSConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(RuntimeExceptionMapper.class);
    }

    /**
     * Mainline to allow a run configuration to be easily used to spin up the example application service
     * locally.
     *
     * @param args use "server" to start the server
     * @throws Exception
     */
    public static void main(String ... args) throws Exception {
        new DBaaSApplication().run(args);
    }

}
