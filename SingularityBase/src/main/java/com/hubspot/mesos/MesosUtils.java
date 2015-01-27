package com.hubspot.mesos;

import static com.hubspot.mesos.SingularityResourceRequest.findNumberResourceRequest;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public final class MesosUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MesosUtils.class);

  public static final String CPUS = "cpus";
  public static final String MEMORY = "mem";
  public static final String PORTS = "ports";

  public static final ImmutableList<String> STANDARD_RESOURCES = ImmutableList.of(CPUS, MEMORY, PORTS);

  private MesosUtils() {
    throw new AssertionError("Do not instantiate!");
  }

  private static double getScalar(Resource resource) {
    checkNotNull(resource, "resource is null");
    checkState(resource.hasScalar(), "resource %s is not a scalar!", resource);

    return resource.getScalar().getValue();
  }

  private static Optional<Double> getScalar(List<Resource> resources, String name) {
    checkNotNull(resources, "resources is null");
    checkNotNull(name, "name is null");

    for (Resource resource : resources) {
      if (resource.hasName() && resource.getName().equals(name) && resource.hasScalar()) {
        return Optional.of(getScalar(resource));
      }
    }

    return Optional.absent();
  }

  private static Optional<Ranges> getRanges(List<Resource> resources, String name) {
    for (Resource r : resources) {
      if (r.hasName() && r.getName().equals(name) && r.hasRanges()) {
        return Optional.of(r.getRanges());
      }
    }

    return Optional.absent();
  }

  private static int getRangeCount(Ranges ranges) {
    checkNotNull(ranges, "ranges is null");
    int totalRanges = 0;
    for (Range range : ranges.getRangeList()) {
      long num = range.getEnd() - range.getBegin();
      totalRanges += num;
    }

    return totalRanges;
  }

  private static Optional<Ranges> getRanges(TaskInfo taskInfo, String name) {
    return getRanges(taskInfo.getResourcesList(), name);
  }

  private static int getNumRanges(List<Resource> resources, String name) {
    Optional<Ranges> ranges = getRanges(resources, name);

    return (ranges.isPresent()) ? getRangeCount(ranges.get()) : 0;
  }

  public static Resource getCpuResource(double cpus) {
    return newScalar(CPUS, cpus);
  }

  public static Resource getMemoryResource(double memory) {
    return newScalar(MEMORY, memory);
  }

  public static int[] getPorts(Resource portsResource, int numPorts) {
    int[] ports = new int[numPorts];
    int idx = 0;

    for (Range r : portsResource.getRanges().getRangeList()) {
      for (long port = r.getBegin(); port <= r.getEnd(); port++) {
        ports[idx++] = Ints.checkedCast(port);

        if (idx >= numPorts) {
          return ports;
        }
      }
    }

    return ports;
  }

  public static List<Long> getAllPorts(TaskInfo taskInfo) {

    final Optional<Ranges> ranges = getRanges(taskInfo, PORTS);
    if (!ranges.isPresent()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<Long> builder = ImmutableList.builder();

    for (Range range : ranges.get().getRangeList()) {
      for (long port = range.getBegin(); port < range.getEnd(); port++) {
        builder.add(port);
      }
    }

    return builder.build();
  }

  public static Optional<Resource> getPortsResource(int numPorts, Offer offer) {
    return getPortsResource(numPorts, offer.getResourcesList());
  }

  public static Optional<Resource> getPortsResource(int numPorts, List<Resource> resources) {
    Optional<Ranges> ranges = getRanges(resources, PORTS);

    if (!ranges.isPresent()) {
      return Optional.absent();
    }

    Ranges.Builder rangesBldr = buildRangeResource(ranges.get(), numPorts);
    return Optional.of(Resource.newBuilder().setType(Type.RANGES).setName(PORTS).setRanges(rangesBldr).build());
  }

  public static Ranges.Builder buildRangeResource(Ranges ranges, int resourceCount) {
    Ranges.Builder rangesBldr = Ranges.newBuilder();

    int resourcesSoFar = 0;

    List<Range> offerRangeList = Lists.newArrayList(ranges.getRangeList());

    Random random = new Random();
    Collections.shuffle(offerRangeList, random);

    for (Range range : offerRangeList) {
      long rangeStartSelection = Math.max(range.getBegin(), range.getEnd() - (resourceCount - resourcesSoFar + 1));

      if (rangeStartSelection != range.getBegin()) {
        int rangeDelta = (int) (rangeStartSelection - range.getBegin()) + 1;
        rangeStartSelection = random.nextInt(rangeDelta) + range.getBegin();
      }

      long rangeEndSelection = Math.min(range.getEnd(), rangeStartSelection + (resourceCount - resourcesSoFar - 1));

      rangesBldr.addRange(Range.newBuilder().setBegin(rangeStartSelection).setEnd(rangeEndSelection));

      resourcesSoFar += rangeEndSelection - rangeStartSelection + 1;

      if (resourcesSoFar == resourceCount) {
        break;
      }
    }

    return rangesBldr;
  }

  private static Resource newScalar(String name, double value) {
    return Resource.newBuilder().setName(name).setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(value).build()).build();
  }

  public static Optional<Double> getNumCpus(Offer offer) {
    return getNumCpus(offer.getResourcesList());
  }

  public static Optional<Double>  getMemory(Offer offer) {
    return getMemory(offer.getResourcesList());
  }

  public static Optional<Double> getNumCpus(List<Resource> resources) {
    return getScalar(resources, CPUS);
  }

  public static Optional<Double>  getMemory(List<Resource> resources) {
    return getScalar(resources, MEMORY);
  }

  public static int getNumPorts(List<Resource> resources) {
    return getNumRanges(resources, PORTS);
  }

  public static int getNumPorts(Offer offer) {
    return getNumPorts(offer.getResourcesList());
  }

  public static boolean doesOfferMatchResources(List<SingularityResourceRequest> requestedResources, List<Resource> offerResources) {

    Optional<Double> numCpus = getNumCpus(offerResources);

    if (SingularityResourceRequest.hasResource(requestedResources, SingularityResourceRequest.CPU_RESOURCE_NAME)) {
      if (!numCpus.isPresent()) {
        return false;
      }

      if (numCpus.get() < findNumberResourceRequest(requestedResources, SingularityResourceRequest.CPU_RESOURCE_NAME, Double.MAX_VALUE).doubleValue()) {
        return false;
      }
    }

    Optional<Double> memory = getMemory(offerResources);

    if (SingularityResourceRequest.hasResource(requestedResources, SingularityResourceRequest.MEMORY_RESOURCE_NAME)) {
      if (!memory.isPresent()) {
        return false;
      }

      if (memory.get() < findNumberResourceRequest(requestedResources, SingularityResourceRequest.MEMORY_RESOURCE_NAME, Double.MAX_VALUE).doubleValue()) {
        return false;
      }
    }

    int numPorts = getNumPorts(offerResources);

    if (SingularityResourceRequest.hasResource(requestedResources, SingularityResourceRequest.PORT_COUNT_RESOURCE_NAME)
        && numPorts < findNumberResourceRequest(requestedResources, SingularityResourceRequest.PORT_COUNT_RESOURCE_NAME, Integer.MAX_VALUE).intValue()) {
      return false;
    }

    return true;
  }

  public static boolean isTaskDone(TaskState state) {
    return state == TaskState.TASK_FAILED || state == TaskState.TASK_LOST || state == TaskState.TASK_KILLED || state == TaskState.TASK_FINISHED;
  }

  public static String getMasterHostAndPort(MasterInfo masterInfo) {
    byte[] fromIp = ByteBuffer.allocate(4).putInt(masterInfo.getIp()).array();

    try {
      return String.format("%s:%s", InetAddresses.fromLittleEndianByteArray(fromIp).getHostAddress(), masterInfo.getPort());
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Optional<Resource> getMatchingResource(Resource toMatch, List<Resource> resources) {
    for (Resource resource : resources) {
      if (toMatch.getName().equals(resource.getName())) {
        return Optional.of(resource);
      }
    }

    return Optional.absent();
  }

  private static final Comparator<Range> RANGE_COMPARATOR = new Comparator<Range>() {
    @Override
    public int compare(Range o1, Range o2) {
      return Longs.compare(o1.getBegin(), o2.getBegin());
    }
  };

  private static Ranges subtractRanges(Ranges ranges, Ranges toSubtract) {
    Ranges.Builder newRanges = Ranges.newBuilder();

    List<Range> sortedRanges = Lists.newArrayList(ranges.getRangeList());
    Collections.sort(sortedRanges, RANGE_COMPARATOR);

    List<Range> subtractRanges = Lists.newArrayList(toSubtract.getRangeList());
    Collections.sort(subtractRanges, RANGE_COMPARATOR);

    int s = 0;

    for (Range range : ranges.getRangeList()) {
      Range.Builder currentRange = range.toBuilder();

      for (int i = s; i < subtractRanges.size(); i++) {
        Range matchedRange = subtractRanges.get(i);

        if (matchedRange.getBegin() < currentRange.getBegin() || matchedRange.getEnd() > currentRange.getEnd()) {
          s = i;
          break;
        }

        currentRange.setEnd(matchedRange.getBegin() - 1);
        if (currentRange.getEnd() >= currentRange.getBegin()) {
          newRanges.addRange(currentRange.build());
        }
        currentRange = Range.newBuilder();
        currentRange.setBegin(matchedRange.getEnd() + 1);
        currentRange.setEnd(range.getEnd());
      }

      if (currentRange.getEnd() >= currentRange.getBegin()) {
        newRanges.addRange(currentRange.build());
      }
    }

    return newRanges.build();
  }

  public static List<Resource> subtractResources(List<Resource> resources, List<Resource> subtract) {
    List<Resource> remaining = Lists.newArrayListWithCapacity(resources.size());

    for (Resource resource : resources) {
      Optional<Resource> matched = getMatchingResource(resource, subtract);

      if (!matched.isPresent()) {
        remaining.add(resource.toBuilder().clone().build());
      } else {
        Resource.Builder resourceBuilder = resource.toBuilder().clone();
        if (resource.hasScalar()) {
          resourceBuilder.setScalar(resource.toBuilder().getScalarBuilder().setValue(resource.getScalar().getValue() - matched.get().getScalar().getValue()).build());
        } else if (resource.hasRanges()) {
          resourceBuilder.setRanges(subtractRanges(resource.getRanges(), matched.get().getRanges()));
        } else {
          throw new IllegalStateException(String.format("Can't subtract non-scalar or range resources %s", resource));
        }

        remaining.add(resourceBuilder.build());
      }
    }

    return remaining;
  }

  public static Path getTaskDirectoryPath(String taskId) {
    return Paths.get(getSafeTaskIdForDirectory(taskId)).toAbsolutePath();
  }

  public static String getSafeTaskIdForDirectory(String taskId) {
    return taskId.replace(":", "_");
  }

  public static Set<String> hasAllResources(List<Resource> resources, Collection<String> resourceNames) {
    ImmutableSet<String> allResourceNames = ImmutableSet.copyOf(resourceNames);
    ImmutableSet<String> presentResourceNames = ImmutableSet.copyOf(Lists.transform(resources, getResourceNameFunction()));

    return ImmutableSet.copyOf(Sets.difference(allResourceNames, presentResourceNames));
  }

  public static Predicate<? super Resource> getFilterStandardAttributesFunction() {
    return new Predicate<Resource>() {

      @Override
      public boolean apply(@Nonnull Resource resource) {
        return !STANDARD_RESOURCES.contains(resource.getName());
      }
    };
  }

  public static String toString(Resource resource) {
    checkNotNull(resource, "resource is null");
    StringBuffer sb = new StringBuffer(resource.getName()).append(": ");

    if (resource.hasRanges()) {
      sb.append(getRangeCount(resource.getRanges()));
    } else if (resource.hasScalar()) {
      sb.append(getScalar(resource));
    } else if (resource.hasSet()) {
      sb.append(resource.getSet().getItemList());
    } else {
      sb.append("Unknown Resource type");
    }

    return sb.toString();
  }

  public static void displayOffer(Offer offer) {
    if (LOG.isDebugEnabled()) {
      checkNotNull(offer, "offer is null");

      List<Resource> resources = offer.getResourcesList();

      Set<String> missing = hasAllResources(resources, STANDARD_RESOURCES);

      if (!missing.isEmpty()) {
        LOG.warn("Resource List from {} is missing one or more standard resources ({})", offer.getSlaveId().getValue(), missing);
      }

      LOG.debug("Offer from {} ({})", offer.getHostname(), offer.getSlaveId().getValue());
      LOG.debug("Attributes: {}", buildAttributeMap(offer.getAttributesList()));
      LOG.debug("Primary Resources: {} CPU(s), {} MB Memory, {} available ports", getScalar(resources, CPUS), getScalar(resources, MEMORY), getNumRanges(resources, PORTS));
      LOG.debug("Additional Resources:");

      for (Protos.Resource resource : Collections2.filter(resources, MesosUtils.getFilterStandardAttributesFunction())) {
        LOG.debug("  - {}", toString(resource));
      }
    }
  }

  public static boolean doesOfferMatchAttributes(Optional<Map<String, String>> taskAttributes, List<Attribute> attributesList) {
    checkNotNull(taskAttributes, "taskAttributes is null");
    checkNotNull(attributesList, "attributesList is null");

    if (!taskAttributes.isPresent()) {
      return true;
    }

    final ImmutableMap<String, String> offerAttributes = buildAttributeMap(attributesList);
    for (Map.Entry<String, String> requestedAttribute : taskAttributes.get().entrySet()) {
      final String key = requestedAttribute.getKey();

      if (!offerAttributes.containsKey(key)) {
        return false;
      }
      else if (!requestedAttribute.getValue().equals(offerAttributes.get(key))) {
        return false;
      }
    }
    return true;
  }

  private static ImmutableMap<String, String> buildAttributeMap(Iterable<Attribute> attributes) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    for (Attribute attribute : attributes) {
      if (attribute.hasText()) {
        builder.put(attribute.getName(), attribute.getText().getValue());
      }
      else if (attribute.hasScalar()) {
        builder.put(attribute.getName(), Double.toString(attribute.getScalar().getValue()));
      }
      else {
        LOG.warn("Attribute {} is not a scalar or text", attribute);
      }
    }

    return builder.build();
  }

  @SuppressWarnings("unused")
  private static Function<? super Attribute, String> getAttributeNameFunction() {
    return new Function<Attribute, String>() {

      @Override
      public String apply(@Nonnull Attribute attribute) {
        return attribute.getName();
      }
    };
  }

  private static Function<? super Resource, String> getResourceNameFunction() {
    return new Function<Resource, String>() {

      @Override
      public String apply(@Nonnull Resource resource) {
        return resource.getName();
      }
    };
  }
}
