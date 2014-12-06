package com.hubspot.singularity.mesos;

import java.util.List;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;

import com.google.common.base.Optional;

/**
 * Any piece of code that wants to participate in calls to the Mesos scheduler framework can extend from
 * this class and then Multibind (see {@link SingularityMesosModule}) to be injected into the {@link SingularityMesosSchedulerDelegator}.
 */
public abstract class SingularitySchedulerParticipant {
  public void registered(Optional<FrameworkID> frameworkId, MasterInfo masterInfo) throws Exception {
  }

  public void disconnected() throws Exception {
  }

  public void resourceOffers(List<Offer> offers) throws Exception {
  }

  public void offerRescinded(OfferID offerId) throws Exception {
  }

  public void statusUpdate(TaskStatus status) throws Exception {
  }

  public void frameworkMessage(ExecutorID executorId, SlaveID slaveId, byte[] data) throws Exception {
  }

  public void slaveLost(SlaveID slaveId) throws Exception {
  }

  public void executorLost(ExecutorID executorId, SlaveID slaveId, int status) throws Exception {
  }

  public void error(String message) throws Exception {
  }
}
