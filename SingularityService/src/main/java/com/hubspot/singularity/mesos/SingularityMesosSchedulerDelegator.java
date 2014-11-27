package com.hubspot.singularity.mesos;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.hubspot.singularity.SingularityAbort;
import com.hubspot.singularity.SingularityAbort.AbortReason;
import com.hubspot.singularity.sentry.SingularityExceptionNotifier;

@Singleton
public class SingularityMesosSchedulerDelegator implements Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(SingularityMesosSchedulerDelegator.class);

  private final SingularityExceptionNotifier exceptionNotifier;

  private final SingularityMesosScheduler scheduler;
  private final SingularityStartup startup;
  private final SingularityAbort abort;

  private final Lock stateLock;
  private final Lock lock;

  private enum SchedulerState {
    STARTUP, RUNNING, STOPPED;
  }

  private volatile SchedulerState state;

  private final List<Protos.TaskStatus> queuedUpdates;

  private Optional<Long> lastOfferTimestamp;
  private final AtomicReference<MasterInfo> masterInfoHolder = new AtomicReference<>();
  private final SchedulerDriverSupplier schedulerDriverSupplier;

  @Inject
  SingularityMesosSchedulerDelegator(SingularityExceptionNotifier exceptionNotifier, SingularityMesosScheduler scheduler, SchedulerDriverSupplier schedulerDriverSupplier,
      SingularityStartup startup, SingularityAbort abort) {
    this.exceptionNotifier = exceptionNotifier;

    this.scheduler = scheduler;
    this.schedulerDriverSupplier = schedulerDriverSupplier;
    this.startup = startup;
    this.abort = abort;

    this.queuedUpdates = Lists.newArrayList();

    this.lock = new ReentrantLock();
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

  // TODO should the lock wait on a timeout and then notify that it's taking a while?

  public void lock() {
    lock.lock();
  }

  public void release() {
    lock.unlock();
  }

  public void notifyStopping() {
    LOG.info("Scheduler is moving to stopped, current state: {}", state);

    state = SchedulerState.STOPPED;

    LOG.info("Scheduler now in state: {}", state);
  }

  private void handleUncaughtSchedulerException(Throwable t) {
    LOG.error("Scheduler threw an uncaught exception - exiting", t);

    exceptionNotifier.notify(t);

    abort.abort(AbortReason.UNRECOVERABLE_ERROR);
  }

  private void startup(SchedulerDriver driver, MasterInfo masterInfo) throws Exception {
    Preconditions.checkState(state == SchedulerState.STARTUP, "Asked to startup - but in invalid state: %s", state.name());

    masterInfoHolder.set(masterInfo);

    startup.startup(masterInfo, driver);

    stateLock.lock(); // ensure we aren't adding queued updates. calls to status updates are now blocked.

    try {
      state = SchedulerState.RUNNING; // calls to resource offers will now block, since we are already scheduler locked.

      for (Protos.TaskStatus status : queuedUpdates) {
        scheduler.statusUpdate(status);
      }

    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    lock();

    LOG.info("Registered driver {}, with frameworkId {} and master {}", driver, frameworkId, masterInfo);

    try {
      schedulerDriverSupplier.setSchedulerDriver(driver);
      startup(driver, masterInfo);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    lock();

    LOG.info("Reregistered driver {}, with master {}", driver, masterInfo);

    try {
      schedulerDriverSupplier.setSchedulerDriver(driver);

      startup(driver, masterInfo);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  public boolean isRunning() {
    return state == SchedulerState.RUNNING;
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    lastOfferTimestamp = Optional.of(System.currentTimeMillis());

    if (!isRunning()) {
      LOG.info(String.format("Scheduler is in state %s, declining %s offer(s)", state.name(), offers.size()));

      for (Protos.Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }

      return;
    }

    lock();

    try {
      scheduler.resourceOffers(offers);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    if (!isRunning()) {
      LOG.info("Ignoring offer rescind message {} because scheduler isn't running ({})", offerId, state);
      return;
    }

    lock();

    try {
      LOG.info("Offer {} rescinded", offerId);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    stateLock.lock();

    try {
      if (!isRunning()) {
        LOG.info("Scheduler is in state {}, queueing an update {} - {} queued updates so far", state.name(), status, queuedUpdates.size());

        queuedUpdates.add(status);

        return;
      }
    } finally {
      stateLock.unlock();
    }

    lock();

    try {
      scheduler.statusUpdate(status);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
    if (!isRunning()) {
      LOG.info("Ignoring framework message because scheduler isn't running ({})", state);
      return;
    }

    lock();

    try {
      LOG.info("Framework message from executor {} on slave {} with data {}", executorId, slaveId, new String(data, UTF_8));
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    if (!isRunning()) {
      LOG.info("Ignoring disconnect because scheduler isn't running ({})", state);
      return;
    }

    LOG.warn("Scheduler/Driver disconnecting");

    lock();

    try {
      schedulerDriverSupplier.setSchedulerDriver(null);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    if (!isRunning()) {
      LOG.info("Ignoring slave lost {} because scheduler isn't running ({})", slaveId, state);
      return;
    }

    lock();

    try {
      scheduler.slaveLost(slaveId);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {
    if (!isRunning()) {
      LOG.info("Ignoring executor lost {} because scheduler isn't running ({})", executorId, state);
      return;
    }

    lock();

    try {
      LOG.warn("Lost an executor {} on slave {} with status {}", executorId, slaveId, status);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    if (!isRunning()) {
      LOG.info("Ignoring error {} because scheduler isn't running ({})", message, state);
      return;
    }

    lock();

    try {
      LOG.error("Aborting due to Mesos error: {}", message);

      abort.abort(AbortReason.MESOS_ERROR);
    } catch (Throwable t) {
      handleUncaughtSchedulerException(t);
    } finally {
      release();
    }
  }

}
