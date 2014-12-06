package com.hubspot.singularity.mesos;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Singleton;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.singularity.SingularityAbort;
import com.hubspot.singularity.SingularityAbort.AbortReason;
import com.hubspot.singularity.sentry.SingularityExceptionNotifier;

@Singleton
public class SingularityMesosSchedulerDelegator implements Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityMesosSchedulerDelegator.class);

  private final SingularityExceptionNotifier exceptionNotifier;

  private final SingularityStartup startup;
  private final SingularityAbort abort;

  private final Lock stateLock;

  private final Lock lock;

  private enum SchedulerState {
    STARTUP, RUNNING, STOPPED;
  }

  private volatile SchedulerState state;

  private final BlockingQueue<Protos.TaskStatus> queuedUpdates = new LinkedBlockingQueue<>();

  private Optional<Long> lastOfferTimestamp;
  private final AtomicReference<MasterInfo> masterInfoHolder = new AtomicReference<>();

  private final SchedulerDriverSupplier schedulerDriverSupplier;
  private final Set<SingularitySchedulerParticipant> participants;

  @Inject
  SingularityMesosSchedulerDelegator(@Named(SingularityMesosModule.SCHEDULER_LOCK_NAME) final Lock lock, final Set<SingularitySchedulerParticipant> participants,
      final SchedulerDriverSupplier schedulerDriverSupplier, final SingularityExceptionNotifier exceptionNotifier, final SingularityStartup startup,
      final SingularityAbort abort) {

    this.lock = lock;
    this.participants = participants;
    this.schedulerDriverSupplier = schedulerDriverSupplier;

    this.exceptionNotifier = exceptionNotifier;

    this.startup = startup;
    this.abort = abort;

    this.stateLock = new ReentrantLock();
    this.state = SchedulerState.STARTUP;
    this.lastOfferTimestamp = Optional.absent();
  }

  public Optional<Long> getLastOfferTimestamp() {
    return lastOfferTimestamp;
  }

  public Optional<MasterInfo> getMaster() {
    return Optional.fromNullable(masterInfoHolder.get());
  }

  public void notifyStopping() {
    LOG.info("Scheduler is moving to stopped, current state: {}", state);
    state = SchedulerState.STOPPED;
  }

  public boolean isRunning() {
    return state == SchedulerState.RUNNING;
  }

  @VisibleForTesting
  public void setRunning() {
    state = SchedulerState.RUNNING;
  }

  private void startup(SchedulerDriver driver, MasterInfo masterInfo) {
    Preconditions.checkState(state == SchedulerState.STARTUP, "Asked to startup - but in invalid state: %s", state.name());

    try {
      startup.startup(masterInfo, driver);

      stateLock.lock(); // ensure we aren't adding queued updates. calls to status updates are now blocked.

      try {
        setRunning(); // calls to resource offers will now block, since we are already scheduler locked.

        for (;;) {
          final Protos.TaskStatus status = queuedUpdates.poll();
          if (status == null) {
            break; // for(;;
          }

          dispatch(new SchedulerDelegate() {
            @Override
            public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
              participant.statusUpdate(status);
            }
          });
        }
      } finally {
        stateLock.unlock();
      }
    } catch (Throwable t) {
      LOG.error("Startup threw an uncaught exception - exiting", t);
      exceptionNotifier.notify(t);
      abort.abort(AbortReason.UNRECOVERABLE_ERROR);
    }
  }

  @Override
  public void registered(SchedulerDriver driver, final FrameworkID frameworkId, final MasterInfo masterInfo) {

    masterInfoHolder.set(masterInfo);
    schedulerDriverSupplier.setSchedulerDriver(driver);

    LOG.info("Registered driver {}, with frameworkId {} and master {}", driver, frameworkId, masterInfo);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.registered(Optional.of(frameworkId), masterInfo);
      }
    });

    startup(driver, masterInfo);
  }

  @Override
  public void reregistered(SchedulerDriver driver, final MasterInfo masterInfo) {

    masterInfoHolder.set(masterInfo);
    schedulerDriverSupplier.setSchedulerDriver(driver);

    LOG.info("Reregistered driver {}, with master {}", driver, masterInfo);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.registered(Optional.<FrameworkID> absent(), masterInfo);
      }
    });

    startup(driver, masterInfo);
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, final List<Offer> offers) {
    lastOfferTimestamp = Optional.of(System.currentTimeMillis());

    if (!isRunning()) {
      LOG.info(String.format("Scheduler is in state %s, declining %s offer(s)", state.name(), offers.size()));

      for (Protos.Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }

      return;
    }

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.resourceOffers(offers);
      }
    });
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, final OfferID offerId) {
    if (!isRunning()) {
      LOG.info("Ignoring offer rescind message {} because scheduler isn't running ({})", offerId, state);
      return;
    }

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.offerRescinded(offerId);
      }
    });
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, final TaskStatus status) {
    stateLock.lock();

    try {
      if (!isRunning()) {
        LOG.info("Scheduler is in state {}, queueing an update {} - {} queued updates so far.", state.name(), status, queuedUpdates.size());

        if (!queuedUpdates.offer(status)) {
          LOG.warn("Could not add status {} to queue!");
        }

        return;
      }
    } finally {
      stateLock.unlock();
    }

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.statusUpdate(status);
      }
    });
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
    if (!isRunning()) {
      LOG.info("Ignoring framework message because scheduler isn't running ({})", state);
      return;
    }

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.frameworkMessage(executorId, slaveId, data);
      }
    });
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    if (!isRunning()) {
      LOG.info("Ignoring disconnect because scheduler isn't running ({})", state);
      return;
    }

    LOG.warn("Scheduler/Driver disconnected");
    schedulerDriverSupplier.setSchedulerDriver(null);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.disconnected();
      }
    });
  }

  @Override
  public void slaveLost(SchedulerDriver driver, final SlaveID slaveId) {
    if (!isRunning()) {
      LOG.info("Ignoring slave lost {} because scheduler isn't running ({})", slaveId, state);
      return;
    }

    LOG.warn("Lost a slave {}", slaveId);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.slaveLost(slaveId);
      }
    });
  }

  @Override
  public void executorLost(SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
    if (!isRunning()) {
      LOG.info("Ignoring executor lost {} because scheduler isn't running ({})", executorId, state);
      return;
    }

    LOG.warn("Lost an executor {} on slave {} with status {}", executorId, slaveId, status);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.executorLost(executorId, slaveId, status);
      }
    });
  }

  @Override
  public void error(SchedulerDriver driver, final String message) {
    if (!isRunning()) {
      LOG.info("Ignoring error {} because scheduler isn't running ({})", message, state);
      return;
    }

    LOG.warn("Error from mesos: {}", message);

    dispatch(new SchedulerDelegate() {
      @Override
      public void delegate(final SingularitySchedulerParticipant participant) throws Exception {
        participant.error(message);
      }
    });

    abort.abort(AbortReason.MESOS_ERROR);
  }

  private interface SchedulerDelegate {
    void delegate(SingularitySchedulerParticipant participant) throws Exception;
  }

  private void dispatch(SchedulerDelegate delegator) {
    lock.lock();

    boolean exceptionCaught = false;
    try {
      for (SingularitySchedulerParticipant participant : participants) {
        try {
          delegator.delegate(participant);
        } catch (Throwable t) {
          LOG.error("Scheduler {} threw an uncaught exception - exiting", participant.getClass().getSimpleName(), t);
          exceptionNotifier.notify(t);
          exceptionCaught = true;
        }
      }
    } finally {
      lock.unlock();
    }

    if (exceptionCaught) {
      abort.abort(AbortReason.UNRECOVERABLE_ERROR);
    }
  }
}
