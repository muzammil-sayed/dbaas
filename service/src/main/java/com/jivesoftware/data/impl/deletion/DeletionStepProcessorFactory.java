package com.jivesoftware.data.impl.deletion;

import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.Map;

public class DeletionStepProcessorFactory {

    private final Map<DeletionStep, DeleteCommandProcessor> stepMap;

    @Inject
    public DeletionStepProcessorFactory(DeleteReadyProcessor deleteReadyProcessor,
                                        DeleteCompleteProcessor deleteCompleteProcessor,
                                        PasswordResetProcessor passwordResetProcessor) {

        stepMap = ImmutableMap.of(DeletionStep.PREPARING, deleteReadyProcessor,
                DeletionStep.DELETING, passwordResetProcessor,
                DeletionStep.RESETTING_PASSWORD, deleteCompleteProcessor);
    }


    public DeleteCommandProcessor getStep(DeletionStep step) {
        return stepMap.get(step);
    }
}
