package com.hubspot.singularity.mesos.scheduler;

import java.util.List;

import org.apache.mesos.Protos;

import com.hubspot.singularity.scheduler.SingularitySchedulerStateCache;

public interface ResourceStrategy
{
  void processOffers(List<Protos.Offer> offers, SingularitySchedulerStateCache stateCache);
}
