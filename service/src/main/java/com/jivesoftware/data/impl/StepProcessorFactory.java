package com.jivesoftware.data.impl;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.data.exceptions.InvalidStepException;

import javax.inject.Inject;
import java.util.Map;

public class StepProcessorFactory {

    private final Map<CreationStep, CreateCommandProcessor> stepMap;

    @Inject
    public StepProcessorFactory(InstanceCreationProcessor instanceCreationProcessor,
                                SchemaCreationProcessor schemaCreationProcessor,
                                CloneProcessor cloneProcessor,
                                ReadyProcessor readyProcessor){

        stepMap = ImmutableMap.of(CreationStep.INSTANCE, instanceCreationProcessor,
                CreationStep.SCHEMA, schemaCreationProcessor,
                CreationStep.CLONE, cloneProcessor,
                CreationStep.INSTANCE_READY, readyProcessor);
    }


    public CreateCommandProcessor getStep(CreationStep step) {
        if(stepMap.get(step) instanceof CreateCommandProcessor) {
            return stepMap.get(step);
        }
        else {
            throw new InvalidStepException(String.format("Received a process request which included" +
                    "an invalid step.  Since this stems from an immutable map something buggy or" +
                    "malicious is happening."));
        }
    }
}
