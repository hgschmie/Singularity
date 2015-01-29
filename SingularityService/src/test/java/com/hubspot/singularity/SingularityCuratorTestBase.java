package com.hubspot.singularity;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import com.google.common.io.Closer;
import com.google.inject.Injector;
import com.hubspot.singularity.scheduler.SingularityTestModule;

public class SingularityCuratorTestBase {

  private final Closer closer = Closer.create();

  private SingularityTestModule singularityTestModule;

  @Before
  public final void curatorSetup() throws Exception {
    singularityTestModule = new SingularityTestModule();


    Injector inj = singularityTestModule.getInjector();
    inj.injectMembers(this);
    closer.register(inj.getInstance(CuratorFramework.class));
    closer.register(inj.getInstance(TestingServer.class));
    singularityTestModule.start();
  }

  @After
  public final void curatorTeardown() throws Exception {
    singularityTestModule.stop();
    closer.close();
  }
}
