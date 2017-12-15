package com.jivesoftware.data.resources;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.jivesoftware.data.DBaaSModule;

public class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MetricRegistry.class).toInstance(new MetricRegistry());
        install(new DBaaSModule());
    }

}
