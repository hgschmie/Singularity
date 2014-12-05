package com.hubspot.singularity.executor;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.hubspot.singularity.executor.config.SingularityExecutorModule;
import com.hubspot.singularity.runner.base.config.SingularityConfigurationLoader;
import com.hubspot.singularity.runner.base.config.SingularityRunnerBaseModule;

public class SingularityExecutorRunnerModule implements Module {

    private final SingularityConfigurationLoader [] configurations;

    public SingularityExecutorRunnerModule(SingularityConfigurationLoader... configurations)
    {
        this.configurations = configurations;
    }

    @Override
    public void configure(final Binder binder) {
        binder.install(new SingularityRunnerBaseModule(configurations));
        binder.install(new SingularityExecutorModule());
    }
}

