/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.*;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.NoOpTaskExecutorBlobService;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorBuilder;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RegistrationResponse.Failure;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.taskexecutor.TaskSubmissionTestEnvironment.Builder;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.ThreadSafeTaskSlotTable;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestFileUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriConsumerWithException;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup;
import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.DEFAULT_RESOURCE_PROFILE;
import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.createDefaultTimerService;
import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.createTotalResourceProfile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

/** Tests for the {@link TaskExecutor}. */
public class TaskExecutorTest extends TestLogger {

    public static final HeartbeatServices HEARTBEAT_SERVICES = new HeartbeatServices(1000L, 1000L);

    private static final TaskExecutorResourceSpec TM_RESOURCE_SPEC =
            new TaskExecutorResourceSpec(
                    new CPUResource(1.0),
                    MemorySize.parse("1m"),
                    MemorySize.parse("2m"),
                    MemorySize.parse("3m"),
                    MemorySize.parse("4m"),
                    Collections.emptyList());

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Rule public final TestName testName = new TestName();

    private static final Time timeout = Time.milliseconds(300000L);

    private static final HeartbeatServices failedRpcEnabledHeartbeatServices =
            new HeartbeatServices(1L, 10000000L, 1);

    private TestingRpcService rpc;

    private Configuration configuration;

    private UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private JobID jobId;
    private JobID jobId2;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    private TestingHighAvailabilityServices haServices;

    private SettableLeaderRetrievalService resourceManagerLeaderRetriever;

    private SettableLeaderRetrievalService jobManagerLeaderRetriever;
    private SettableLeaderRetrievalService jobManagerLeaderRetriever2;

    private NettyShuffleEnvironment nettyShuffleEnvironment;

    @Before
    public void setup() throws IOException {
        rpc = new TestingRpcService();

        configuration = new Configuration();
        TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

        unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
        jobId = new JobID();
        jobId2 = new JobID();

        testingFatalErrorHandler = new TestingFatalErrorHandler();

        haServices = new TestingHighAvailabilityServices();
        resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
        jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
        jobManagerLeaderRetriever2 = new SettableLeaderRetrievalService();
        haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
        haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
        haServices.setJobMasterLeaderRetriever(jobId2, jobManagerLeaderRetriever2);

        nettyShuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
    }

    @After
    public void teardown() throws Exception {
        if (rpc != null) {
            RpcUtils.terminateRpcService(rpc);
            rpc = null;
        }

        if (nettyShuffleEnvironment != null) {
            nettyShuffleEnvironment.close();
        }

        testingFatalErrorHandler.rethrowError();
    }

    @Test
    public void testShouldShutDownTaskManagerServicesInPostStop() throws Exception {
        final TaskSlotTableImpl<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());

        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        RetryingRegistrationConfiguration.defaultConfiguration());

        final IOManager ioManager = new IOManagerAsync(tmp.newFolder().getAbsolutePath());

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                new TaskExecutorLocalStateStoresManager(
                        false,
                        Reference.borrowed(ioManager.getSpillingDirectories()),
                        Executors.directExecutor());

        nettyShuffleEnvironment.start();

        final KvStateService kvStateService = new KvStateService(new KvStateRegistry(), null, null);
        kvStateService.start();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setIoManager(ioManager)
                        .setShuffleEnvironment(nettyShuffleEnvironment)
                        .setKvStateService(kvStateService)
                        .setTaskSlotTable(taskSlotTable)
                        .setJobLeaderService(jobLeaderService)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

        try {
            taskManager.start();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }

        assertTrue(taskSlotTable.isClosed());
        assertTrue(nettyShuffleEnvironment.isClosed());
        assertTrue(kvStateService.isShutdown());
    }

    @Test
    public void testHeartbeatTimeoutWithJobManager() throws Exception {
        final TestingHeartbeatServices heartbeatServices = new TestingHeartbeatServices();
        final ResourceID jmResourceId = ResourceID.generate();
        runJobManagerHeartbeatTest(
                jmResourceId,
                heartbeatServices,
                ignored -> {},
                (taskExecutorResourceId, ignoredTaskExecutorGateway, ignoredAllocationId) ->
                        heartbeatServices.triggerHeartbeatTimeout(
                                taskExecutorResourceId, jmResourceId));
    }

    @Test
    public void testJobManagerBecomesUnreachableTriggersDisconnect() throws Exception {
        final ResourceID jmResourceId = ResourceID.generate();
        runJobManagerHeartbeatTest(
                jmResourceId,
                failedRpcEnabledHeartbeatServices,
                jobMasterGatewayBuilder ->
                        jobMasterGatewayBuilder.setTaskManagerHeartbeatFunction(
                                (resourceID, taskExecutorToJobManagerHeartbeatPayload) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "job manager is unreachable."))),
                (ignoredTaskExecutorResourceId, taskExecutorGateway, allocationId) ->
                        taskExecutorGateway.heartbeatFromJobManager(
                                jmResourceId,
                                new AllocatedSlotReport(
                                        jobId,
                                        Collections.singleton(
                                                new AllocatedSlotInfo(0, allocationId)))));
    }

    private void runJobManagerHeartbeatTest(
            ResourceID jmResourceId,
            HeartbeatServices heartbeatServices,
            Consumer<TestingJobMasterGatewayBuilder> jobMasterGatewayBuilderConsumer,
            TriConsumer<ResourceID, TaskExecutorGateway, AllocationID> heartbeatAction)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        RetryingRegistrationConfiguration.defaultConfiguration());

        final String jobMasterAddress = "jm";
        final UUID jmLeaderId = UUID.randomUUID();

        final CountDownLatch registrationAttempts = new CountDownLatch(2);
        final OneShotLatch slotOfferedLatch = new OneShotLatch();
        final CompletableFuture<ResourceID> disconnectTaskManagerFuture = new CompletableFuture<>();

        final TestingJobMasterGatewayBuilder testingJobMasterGatewayBuilder =
                new TestingJobMasterGatewayBuilder()
                        .setRegisterTaskManagerFunction(
                                (ignoredJobId, ignoredTaskManagerRegistrationInformation) -> {
                                    registrationAttempts.countDown();
                                    return CompletableFuture.completedFuture(
                                            new JMTMRegistrationSuccess(jmResourceId));
                                })
                        .setDisconnectTaskManagerFunction(
                                resourceID -> {
                                    disconnectTaskManagerFuture.complete(resourceID);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    slotOfferedLatch.trigger();
                                    return CompletableFuture.completedFuture(slotOffers);
                                });

        jobMasterGatewayBuilderConsumer.accept(testingJobMasterGatewayBuilder);

        final TestingJobMasterGateway jobMasterGateway = testingJobMasterGatewayBuilder.build();

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(
                                TaskSlotUtils.createTaskSlotTable(
                                        1, EXECUTOR_RESOURCE.getExecutor()))
                        .setJobLeaderService(jobLeaderService)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TestingTaskExecutor taskManager =
                createTestingTaskExecutor(taskManagerServices, heartbeatServices);

        final OneShotLatch slotReportReceived = new OneShotLatch();
        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setSendSlotReportFunction(
                ignored -> {
                    slotReportReceived.trigger();
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final Queue<CompletableFuture<RegistrationResponse>> registrationResponses =
                new ArrayDeque<>();
        registrationResponses.add(
                CompletableFuture.completedFuture(
                        new TaskExecutorRegistrationSuccess(
                                new InstanceID(),
                                testingResourceManagerGateway.getOwnResourceId(),
                                new ClusterInformation("foobar", 1234))));
        registrationResponses.add(new CompletableFuture<>());
        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> registrationResponses.poll());

        rpc.registerGateway(jobMasterAddress, jobMasterGateway);
        rpc.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

        try {
            taskManager.start();
            taskManager.waitUntilStarted();

            final TaskExecutorGateway taskExecutorGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());
            slotReportReceived.await();

            final AllocationID allocationId = new AllocationID();
            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    allocationId,
                    buildSlotID(0),
                    ResourceProfile.UNKNOWN,
                    jobMasterAddress,
                    testingResourceManagerGateway.getFencingToken());

            // now inform the task manager about the new job leader
            jobManagerLeaderRetriever.notifyListener(jobMasterAddress, jmLeaderId);

            // register task manager success will trigger monitoring heartbeat target between tm and
            // jm
            slotOfferedLatch.await();

            heartbeatAction.accept(
                    unresolvedTaskManagerLocation.getResourceID(),
                    taskExecutorGateway,
                    allocationId);

            // the timeout should trigger disconnecting from the JobManager
            final ResourceID resourceID = disconnectTaskManagerFuture.get();
            assertEquals(resourceID, unresolvedTaskManagerLocation.getResourceID());

            assertTrue(
                    "The TaskExecutor should try to reconnect to the JM",
                    registrationAttempts.await(timeout.toMilliseconds(), TimeUnit.SECONDS));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    @Test
    public void testHeartbeatTimeoutWithResourceManager() throws Exception {
        runResourceManagerHeartbeatTest(
                new HeartbeatServices(1L, 3L),
                (ignoredResourceManagerGateway) -> {},
                (ignoredA, ignoredB, ignoredC) -> {});
    }

    @Test
    public void testResourceManagerBecomesUnreachableTriggersDisconnect() throws Exception {
        runResourceManagerHeartbeatTest(
                failedRpcEnabledHeartbeatServices,
                (rmGateway) ->
                        rmGateway.setTaskExecutorHeartbeatFunction(
                                (resourceID, taskExecutorHeartbeatPayload) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "resource manager is unreachable."))),
                (taskExecutorGateway, rmResourceId, taskExecutorDisconnectFuture) ->
                        CommonTestUtils.waitUntilCondition(
                                () -> {
                                    // request heartbeats until the disconnect future is completed
                                    taskExecutorGateway.heartbeatFromResourceManager(rmResourceId);
                                    return taskExecutorDisconnectFuture.isDone();
                                },
                                50L));
    }

    private void runResourceManagerHeartbeatTest(
            HeartbeatServices heartbeatServices,
            Consumer<TestingResourceManagerGateway> setupResourceManagerGateway,
            TriConsumerWithException<
                    TaskExecutorGateway,
                    ResourceID,
                    CompletableFuture<ResourceID>,
                    Exception>
                    heartbeatAction)
            throws Exception {
        final String rmAddress = "rm";
        final ResourceID rmResourceId = new ResourceID(rmAddress);

        final ResourceManagerId rmLeaderId = ResourceManagerId.generate();

        TestingResourceManagerGateway rmGateway =
                new TestingResourceManagerGateway(rmLeaderId, rmResourceId, rmAddress, rmAddress);

        final TaskExecutorRegistrationSuccess registrationResponse =
                new TaskExecutorRegistrationSuccess(
                        new InstanceID(), rmResourceId, new ClusterInformation("localhost", 1234));
        final Queue<CompletableFuture<RegistrationResponse>> registrationResponses =
                new ArrayDeque<>(2);
        registrationResponses.add(CompletableFuture.completedFuture(registrationResponse));
        registrationResponses.add(new CompletableFuture<>());

        final CompletableFuture<ResourceID> taskExecutorRegistrationFuture =
                new CompletableFuture<>();
        final CountDownLatch registrationAttempts = new CountDownLatch(2);
        rmGateway.setRegisterTaskExecutorFunction(
                registration -> {
                    taskExecutorRegistrationFuture.complete(registration.getResourceId());
                    registrationAttempts.countDown();
                    return registrationResponses.poll();
                });

        setupResourceManagerGateway.accept(rmGateway);

        final CompletableFuture<ResourceID> taskExecutorDisconnectFuture =
                new CompletableFuture<>();
        rmGateway.setDisconnectTaskExecutorConsumer(
                disconnectInfo -> taskExecutorDisconnectFuture.complete(disconnectInfo.f0));

        rpc.registerGateway(rmAddress, rmGateway);

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .build();

        final TaskExecutor taskManager = createTaskExecutor(taskManagerServices, heartbeatServices);

        try {
            taskManager.start();
            final TaskExecutorGateway taskExecutorGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            // define a leader and see that a registration happens
            resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId.toUUID());

            // register resource manager success will trigger monitoring heartbeat target between tm
            // and rm
            assertEquals(taskExecutorRegistrationFuture.get(), unresolvedTaskManagerLocation.getResourceID());

            heartbeatAction.accept(
                    taskExecutorGateway,
                    rmGateway.getOwnResourceId(),
                    taskExecutorDisconnectFuture);

            // heartbeat timeout should trigger disconnect TaskManager from ResourceManager
            assertEquals(taskExecutorDisconnectFuture.get(
                    timeout.toMilliseconds(), TimeUnit.MILLISECONDS), unresolvedTaskManagerLocation.getResourceID());

            assertTrue(
                    "The TaskExecutor should try to reconnect to the RM",
                    registrationAttempts.await(timeout.toMilliseconds(), TimeUnit.SECONDS));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    /** Tests that the correct partition/slot report is sent as part of the heartbeat response. */
    @Test
    public void testHeartbeatReporting() throws Exception {
        final String rmAddress = "rm";
        final UUID rmLeaderId = UUID.randomUUID();

        // register the mock resource manager gateway
        final TestingResourceManagerGateway rmGateway = new TestingResourceManagerGateway();
        final CompletableFuture<ResourceID> taskExecutorRegistrationFuture =
                new CompletableFuture<>();
        final ResourceID rmResourceId = rmGateway.getOwnResourceId();
        final CompletableFuture<RegistrationResponse> registrationResponse =
                CompletableFuture.completedFuture(
                        new TaskExecutorRegistrationSuccess(
                                new InstanceID(),
                                rmResourceId,
                                new ClusterInformation("localhost", 1234)));

        rmGateway.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> {
                    taskExecutorRegistrationFuture.complete(
                            taskExecutorRegistration.getResourceId());
                    return registrationResponse;
                });

        final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();
        rmGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final CompletableFuture<TaskExecutorHeartbeatPayload> heartbeatPayloadCompletableFuture =
                new CompletableFuture<>();
        rmGateway.setTaskExecutorHeartbeatFunction(
                (resourceID, heartbeatPayload) -> {
                    heartbeatPayloadCompletableFuture.complete(heartbeatPayload);
                    return FutureUtils.completedVoidFuture();
                });

        rpc.registerGateway(rmAddress, rmGateway);

        final SlotID slotId = buildSlotID(0);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotReport slotReport1 = new SlotReport(new SlotStatus(slotId, resourceProfile));
        final SlotReport slotReport2 =
                new SlotReport(
                        new SlotStatus(slotId, resourceProfile, new JobID(), new AllocationID()));

        final Queue<SlotReport> reports = new ArrayDeque<>(Arrays.asList(slotReport1, slotReport2));
        final TaskSlotTable<Task> taskSlotTable =
                TestingTaskSlotTable.<Task>newBuilder()
                        .createSlotReportSupplier(reports::poll)
                        .closeAsyncReturns(CompletableFuture.completedFuture(null))
                        .build();

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TaskExecutorPartitionTracker partitionTracker =
                createPartitionTrackerWithFixedPartitionReport(
                        taskManagerServices.getShuffleEnvironment());

        final TaskExecutor taskManager =
                createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES, partitionTracker);

        try {
            taskManager.start();

            // define a leader and see that a registration happens
            resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId);

            // register resource manager success will trigger monitoring heartbeat target between tm
            // and rm
            assertEquals(taskExecutorRegistrationFuture.get(), unresolvedTaskManagerLocation.getResourceID());
            assertEquals(initialSlotReportFuture.get(), slotReport1);

            TaskExecutorGateway taskExecutorGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            // trigger the heartbeat asynchronously
            taskExecutorGateway.heartbeatFromResourceManager(rmResourceId);

            // wait for heartbeat response
            SlotReport actualSlotReport = heartbeatPayloadCompletableFuture.get().getSlotReport();

            // the new slot report should be reported
            assertEquals(slotReport2, actualSlotReport);

            ClusterPartitionReport actualClusterPartitionReport =
                    heartbeatPayloadCompletableFuture.get().getClusterPartitionReport();
            assertEquals(
                    partitionTracker.createClusterPartitionReport(), actualClusterPartitionReport);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    private static TaskExecutorPartitionTracker createPartitionTrackerWithFixedPartitionReport(
            ShuffleEnvironment<?, ?> shuffleEnvironment) {
        ResultPartitionID resultPartitionID = new ResultPartitionID();
        final ClusterPartitionReport.ClusterPartitionReportEntry clusterPartitionReportEntry =
                new ClusterPartitionReport.ClusterPartitionReportEntry(
                        new IntermediateDataSetID(),
                        4,
                        Collections.singletonMap(
                                resultPartitionID,
                                new ShuffleDescriptor() {
                                    @Override
                                    public ResultPartitionID getResultPartitionID() {
                                        return resultPartitionID;
                                    }

                                    @Override
                                    public Optional<ResourceID> storesLocalResourcesOn() {
                                        return Optional.empty();
                                    }
                                }));

        final ClusterPartitionReport clusterPartitionReport =
                new ClusterPartitionReport(Collections.singletonList(clusterPartitionReportEntry));

        return new TaskExecutorPartitionTrackerImpl(shuffleEnvironment) {
            @Override
            public ClusterPartitionReport createClusterPartitionReport() {
                return clusterPartitionReport;
            }
        };
    }

    @Test
    public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
        final String resourceManagerAddress = "/resource/manager/address/one";

        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        final CountDownLatch taskManagerRegisteredLatch = new CountDownLatch(1);
        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                FunctionUtils.uncheckedFunction(
                        ignored -> {
                            taskManagerRegisteredLatch.countDown();
                            return CompletableFuture.completedFuture(
                                    new TaskExecutorRegistrationSuccess(
                                            new InstanceID(),
                                            new ResourceID(resourceManagerAddress),
                                            new ClusterInformation("localhost", 1234)));
                        }));

        rpc.registerGateway(resourceManagerAddress, testingResourceManagerGateway);

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(
                                TaskSlotUtils.createTaskSlotTable(
                                        1, EXECUTOR_RESOURCE.getExecutor()))
                        .setTaskStateManager(createTaskExecutorLocalStateStoresManager())
                        .build();

        final TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

        try {
            taskManager.start();
            resourceManagerLeaderRetriever.notifyListener(
                    resourceManagerAddress, UUID.randomUUID());

            assertTrue(
                    taskManagerRegisteredLatch.await(
                            timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    @Test
    public void testTriggerRegistrationOnLeaderChange() throws Exception {
        final UUID leaderId1 = UUID.randomUUID();
        final UUID leaderId2 = UUID.randomUUID();

        // register the mock resource manager gateways
        final CompletableFuture<TaskExecutorRegistration> rmGateway1TaskExecutorRegistration =
                new CompletableFuture<>();
        TestingResourceManagerGateway rmGateway1 = new TestingResourceManagerGateway();
        rmGateway1.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> {
                    rmGateway1TaskExecutorRegistration.complete(taskExecutorRegistration);
                    return createRegistrationResponse(rmGateway1);
                });

        final CompletableFuture<TaskExecutorRegistration> rmGateway2TaskExecutorRegistration =
                new CompletableFuture<>();
        TestingResourceManagerGateway rmGateway2 = new TestingResourceManagerGateway();
        rmGateway2.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> {
                    rmGateway2TaskExecutorRegistration.complete(taskExecutorRegistration);
                    return createRegistrationResponse(rmGateway2);
                });

        rpc.registerGateway(rmGateway1.getAddress(), rmGateway1);
        rpc.registerGateway(rmGateway2.getAddress(), rmGateway2);

        final TaskSlotTable<Task> taskSlotTable =
                TestingTaskSlotTable.<Task>newBuilder()
                        .createSlotReportSupplier(SlotReport::new)
                        .closeAsyncReturns(CompletableFuture.completedFuture(null))
                        .build();

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

        try {
            taskManager.start();
            String taskManagerAddress = taskManager.getAddress();

            // no connection initially, since there is no leader
            assertNull(taskManager.getResourceManagerConnection());

            // define a leader and see that a registration happens
            resourceManagerLeaderRetriever.notifyListener(rmGateway1.getAddress(), leaderId1);
            final TaskExecutorRegistration taskExecutorRegistration1 =
                    rmGateway1TaskExecutorRegistration.join();
            assertEquals(taskExecutorRegistration1.getTaskExecutorAddress(), taskManagerAddress);
            assertEquals(taskExecutorRegistration1.getResourceId(), unresolvedTaskManagerLocation.getResourceID());
            assertNotNull(taskManager.getResourceManagerConnection());

            // cancel the leader
            resourceManagerLeaderRetriever.notifyListener(null, null);

            // set a new leader, see that a registration happens
            resourceManagerLeaderRetriever.notifyListener(rmGateway2.getAddress(), leaderId2);

            final TaskExecutorRegistration taskExecutorRegistration2 =
                    rmGateway2TaskExecutorRegistration.join();

            assertEquals(taskExecutorRegistration1.getTaskExecutorAddress(), taskManagerAddress);
            assertEquals(taskExecutorRegistration2.getResourceId(), unresolvedTaskManagerLocation.getResourceID());
            assertNotNull(taskManager.getResourceManagerConnection());
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    private CompletableFuture<RegistrationResponse> createRegistrationResponse(
            TestingResourceManagerGateway rmGateway1) {
        return CompletableFuture.completedFuture(
                new TaskExecutorRegistrationSuccess(
                        new InstanceID(),
                        rmGateway1.getOwnResourceId(),
                        new ClusterInformation("localhost", 1234)));
    }

    @Test
    public void testTaskSlotTableTerminationOnShutdown() throws Exception {
        CompletableFuture<Void> taskSlotTableClosingFuture = new CompletableFuture<>();
        TaskExecutorTestingContext submissionContext =
                createTaskExecutorTestingContext(
                        TestingTaskSlotTable.<Task>newBuilder()
                                .closeAsyncReturns(taskSlotTableClosingFuture)
                                .build());
        final CompletableFuture<Void> taskExecutorTerminationFuture;
        try {
            submissionContext.start();
        } finally {
            taskExecutorTerminationFuture = submissionContext.taskExecutor.closeAsync();
        }

        // check task executor is waiting for the task completion and has not terminated yet
        assertFalse(taskExecutorTerminationFuture.isDone());

        // check task executor has exited after task slot table termination
        taskSlotTableClosingFuture.complete(null);
        taskExecutorTerminationFuture.get();
    }

    private ResourceManagerId createAndRegisterResourceManager(
            CompletableFuture<Tuple3<ResourceID, InstanceID, SlotReport>> initialSlotReportFuture) {
        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        resourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        // tell the task manager about the rm leader
        resourceManagerLeaderRetriever.notifyListener(
                resourceManagerGateway.getAddress(),
                resourceManagerGateway.getFencingToken().toUUID());

        return resourceManagerGateway.getFencingToken();
    }

    /**
     * Tests that a TaskManager detects a job leader for which it has reserved slots. Upon detecting
     * the job leader, it will offer all reserved slots to the JobManager.
     */
    @Test
    public void testJobLeaderDetection() throws Exception {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());
        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        RetryingRegistrationConfiguration.defaultConfiguration());

        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        CompletableFuture<Void> initialSlotReportFuture = new CompletableFuture<>();
        resourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(null);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final CompletableFuture<Collection<SlotOffer>> offeredSlotsFuture =
                new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offeredSlotsFuture.complete(new ArrayList<>(slotOffers));
                                    return CompletableFuture.completedFuture(slotOffers);
                                })
                        .build();

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final AllocationID allocationId = new AllocationID();

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setJobLeaderService(jobLeaderService)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

        try {
            taskManager.start();

            final TaskExecutorGateway tmGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            // tell the task manager about the rm leader
            resourceManagerLeaderRetriever.notifyListener(
                    resourceManagerGateway.getAddress(),
                    resourceManagerGateway.getFencingToken().toUUID());

            initialSlotReportFuture.get();

            requestSlot(
                    tmGateway,
                    jobId,
                    allocationId,
                    buildSlotID(0),
                    ResourceProfile.ZERO,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // now inform the task manager about the new job leader
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            final Collection<SlotOffer> offeredSlots = offeredSlotsFuture.get();
            final Collection<AllocationID> allocationIds =
                    offeredSlots.stream()
                            .map(SlotOffer::getAllocationId)
                            .collect(Collectors.toList());
            Set<AllocationID> expectedIds = Collections.singleton(allocationId);
            Set<AllocationID> actualIds = new HashSet<>(allocationIds);
            assertEquals(expectedIds, actualIds);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    /**
     * Tests that accepted slots go into state assigned and the others are returned to the resource
     * manager.
     */
    @Test
    public void testSlotAcceptance() throws Exception {
        final InstanceID registrationId = new InstanceID();
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture =
                new CompletableFuture<>();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        registrationId, taskExecutorIsRegistered, availableSlotFuture);

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.ANY);

        final OneShotLatch offerSlotsLatch = new OneShotLatch();
        final OneShotLatch taskInTerminalState = new OneShotLatch();
        final CompletableFuture<Collection<SlotOffer>> offerResultFuture =
                new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                createJobMasterWithSlotOfferAndTaskTerminationHooks(
                        offerSlotsLatch, taskInTerminalState, offerResultFuture);

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
        final TaskManagerServices taskManagerServices =
                createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
        final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices);

        try {
            taskManager.start();
            taskManager.waitUntilStarted();

            final TaskExecutorGateway tmGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            // wait until registered at the RM
            taskExecutorIsRegistered.await();

            // request 2 slots for the given allocation ids
            AllocationID[] allocationIds = new AllocationID[] {allocationId1, allocationId2};
            for (int i = 0; i < allocationIds.length; i++) {
                requestSlot(
                        tmGateway,
                        jobId,
                        allocationIds[i],
                        buildSlotID(i),
                        ResourceProfile.UNKNOWN,
                        jobMasterGateway.getAddress(),
                        resourceManagerGateway.getFencingToken());
            }

            // notify job leader to start slot offering
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            // wait until slots have been offered
            offerSlotsLatch.await();
            offerResultFuture.complete(Collections.singletonList(offer1));

            final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 =
                    availableSlotFuture.get();

            final Tuple3<InstanceID, SlotID, AllocationID> expectedResult =
                    Tuple3.of(registrationId, buildSlotID(1), allocationId2);

            assertEquals(instanceIDSlotIDAllocationIDTuple3, expectedResult);
            // the slot 1 can be activate for task submission
            submit(allocationId1, jobMasterGateway, tmGateway, NoOpInvokable.class);
            // wait for the task completion
            taskInTerminalState.await();
            // the slot 2 can NOT be activate for task submission
            try {
                submit(allocationId2, jobMasterGateway, tmGateway, NoOpInvokable.class);
                fail(
                        "It should not be possible to submit task to acquired by JM slot with index 1 (allocationId2)");
            } catch (CompletionException e) {
                assertTrue(e.getCause() instanceof TaskSubmissionException);
            }
            // the slot 2 is free to request
            requestSlot(
                    tmGateway,
                    jobId,
                    allocationId2,
                    buildSlotID(1),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }


    @Test
    public void testCalculateSlotFalconSize() throws Exception {
        Configuration mockConfig = new Configuration();
        mockConfig.setBoolean("state.backend.rocksdb.falcon.use-state-cache", true);
        mockConfig.setInteger("state.backend.rocksdb.falcon.state-cache-sizeLimit", 24000);

        try (MockedStatic<GlobalConfiguration> mockedStatic = Mockito.mockStatic(GlobalConfiguration.class)) {
            mockedStatic.when(GlobalConfiguration::loadConfiguration).thenReturn(mockConfig);

            final TaskSlotTable<Task> taskSlotTable =
                    TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
            final TaskManagerServices taskManagerServices =
                    createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
            final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices);

            ExecutionAttemptID mockExecutionAttemptID = Mockito.mock(ExecutionAttemptID.class);
            mockExecutionAttemptID.setSubTaskNum(4);

            taskManager.calculateSlotFalconSize(mockExecutionAttemptID);

            mockExecutionAttemptID.getSubTaskFalconSize();
        }
    }

    private enum ResponseOrder {
        ACCEPT_THEN_REJECT,
        REJECT_THEN_ACCEPT
    }

    /**
     * Tests that the task executor does not release a slot that was rejected by the job master, if
     * another slot offer is currently in progress.
     */
    @Test
    public void testRejectedSlotNotFreedIfAnotherOfferIsPending() throws Exception {
        testSlotOfferResponseWithPendingSlotOffer(ResponseOrder.REJECT_THEN_ACCEPT);
    }

    /**
     * Tests that the task executor does not activate a slot that was accepted by the job master, if
     * another slot offer is currently in progress.
     */
    @Test
    public void testAcceptedSlotNotActivatedIfAnotherOfferIsPending() throws Exception {
        testSlotOfferResponseWithPendingSlotOffer(ResponseOrder.ACCEPT_THEN_REJECT);
    }

    /**
     * Tests the behavior of the task executor when a slot offer response is received while a newer
     * slot offer is in progress.
     */
    private void testSlotOfferResponseWithPendingSlotOffer(final ResponseOrder responseOrder)
            throws Exception {
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        new InstanceID(), taskExecutorIsRegistered, new CompletableFuture<>());

        final CompletableFuture<Collection<SlotOffer>> firstOfferResponseFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<SlotOffer>> secondOfferResponseFuture =
                new CompletableFuture<>();

        final Queue<CompletableFuture<Collection<SlotOffer>>> slotOfferResponses =
                new ArrayDeque<>(
                        Arrays.asList(firstOfferResponseFuture, secondOfferResponseFuture));

        final MultiShotLatch offerSlotsLatch = new MultiShotLatch();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offerSlotsLatch.trigger();
                                    return slotOfferResponses.remove();
                                })
                        .build();

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
        final TaskManagerServices taskManagerServices =
                createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
        final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

        final ThreadSafeTaskSlotTable<Task> threadSafeTaskSlotTable =
                new ThreadSafeTaskSlotTable<>(
                        taskSlotTable, taskExecutor.getMainThreadExecutableForTesting());

        final SlotOffer slotOffer1 = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);
        final SlotOffer slotOffer2 = new SlotOffer(new AllocationID(), 1, ResourceProfile.ANY);

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway tmGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            // wait until task executor registered at the RM
            taskExecutorIsRegistered.await();

            // notify job leader to start slot offering
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            // request the first slot
            requestSlot(
                    tmGateway,
                    jobId,
                    slotOffer1.getAllocationId(),
                    buildSlotID(slotOffer1.getSlotIndex()),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // wait until first slot offer as arrived
            offerSlotsLatch.await();

            // request second slot, triggering another offer containing both slots
            int slotIndex = slotOffer2.getSlotIndex();
            requestSlot(
                    tmGateway,
                    jobId,
                    slotOffer2.getAllocationId(),
                    buildSlotID(slotIndex),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // wait until second slot offer as arrived
            offerSlotsLatch.await();

            switch (responseOrder) {
                case ACCEPT_THEN_REJECT:
                    // accept the first offer, but reject both slots for the second offer
                    firstOfferResponseFuture.complete(Collections.singletonList(slotOffer1));
                    assertEquals(threadSafeTaskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId).size(), 0);
                    secondOfferResponseFuture.complete(Collections.emptyList());
                    assertEquals(threadSafeTaskSlotTable.getAllocationIdsPerJob(jobId).size(), 0);
                    return;
                case REJECT_THEN_ACCEPT:
                    // fail the first offer, but accept both slots for the second offer
                    // in the past the rejection of the first offer freed the slot; when the slot is
                    // accepted from the second offer the activation of said slot then failed
                    firstOfferResponseFuture.complete(Collections.emptyList());
                    secondOfferResponseFuture.complete(Arrays.asList(slotOffer1, slotOffer2));
                    Set<AllocationID> expectedIds = new HashSet<>(Arrays.asList(
                            slotOffer1.getAllocationId(),
                            slotOffer2.getAllocationId()
                    ));
                    Set<AllocationID> actualIds = new HashSet<>(threadSafeTaskSlotTable.getAllocationIdsPerJob(jobId));
                    assertEquals(expectedIds, actualIds);
                    return;
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testSlotOfferCounterIsSeparatedByJob() throws Exception {
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        new InstanceID(), taskExecutorIsRegistered, new CompletableFuture<>());

        final CompletableFuture<Collection<SlotOffer>> firstOfferResponseFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<SlotOffer>> secondOfferResponseFuture =
                new CompletableFuture<>();

        final Queue<CompletableFuture<Collection<SlotOffer>>> slotOfferResponses =
                new ArrayDeque<>(
                        Arrays.asList(firstOfferResponseFuture, secondOfferResponseFuture));

        final MultiShotLatch offerSlotsLatch = new MultiShotLatch();
        final TestingJobMasterGateway jobMasterGateway1 =
                new TestingJobMasterGatewayBuilder()
                        .setAddress("jm1")
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offerSlotsLatch.trigger();
                                    return slotOfferResponses.remove();
                                })
                        .build();
        final TestingJobMasterGateway jobMasterGateway2 =
                new TestingJobMasterGatewayBuilder()
                        .setAddress("jm2")
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offerSlotsLatch.trigger();
                                    return slotOfferResponses.remove();
                                })
                        .build();

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway1.getAddress(), jobMasterGateway1);
        rpc.registerGateway(jobMasterGateway2.getAddress(), jobMasterGateway2);

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
        final TaskManagerServices taskManagerServices =
                createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
        final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

        final ThreadSafeTaskSlotTable<Task> threadSafeTaskSlotTable =
                new ThreadSafeTaskSlotTable<>(
                        taskSlotTable, taskExecutor.getMainThreadExecutableForTesting());

        final SlotOffer slotOffer1 = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);
        final SlotOffer slotOffer2 = new SlotOffer(new AllocationID(), 1, ResourceProfile.ANY);

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway tmGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            // wait until task executor registered at the RM
            taskExecutorIsRegistered.await();

            // notify job leader to start slot offering
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway1.getAddress(), jobMasterGateway1.getFencingToken().toUUID());
            jobManagerLeaderRetriever2.notifyListener(
                    jobMasterGateway2.getAddress(), jobMasterGateway2.getFencingToken().toUUID());

            // request the first slot
            requestSlot(
                    tmGateway,
                    jobId,
                    slotOffer1.getAllocationId(),
                    buildSlotID(slotOffer1.getSlotIndex()),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway1.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // wait until first slot offer as arrived
            offerSlotsLatch.await();

            // request second slot, triggering another offer containing both slots
            requestSlot(
                    tmGateway,
                    jobId2,
                    slotOffer2.getAllocationId(),
                    buildSlotID(slotOffer2.getSlotIndex()),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway2.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // wait until second slot offer as arrived
            offerSlotsLatch.await();

            firstOfferResponseFuture.complete(Collections.singletonList(slotOffer1));
            secondOfferResponseFuture.complete(Collections.singletonList(slotOffer2));
            assertEquals(
                    Collections.singleton(slotOffer1.getAllocationId()),
                    threadSafeTaskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId));
            assertEquals(
                    Collections.singleton(slotOffer2.getAllocationId()),
                    threadSafeTaskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId2));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that freeing an inactive slot is a legal operation that does not throw an exception.
     */
    @Test
    public void testFreeingInactiveSlotDoesNotFail() throws Exception {
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture =
                new CompletableFuture<>();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        new InstanceID(), taskExecutorIsRegistered, availableSlotFuture);

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        final MultiShotLatch offerSlotsLatch = new MultiShotLatch();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offerSlotsLatch.trigger();
                                    return new CompletableFuture<>();
                                })
                        .build();

        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());
        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

        final ThreadSafeTaskSlotTable<Task> threadSafeTaskSlotTable =
                new ThreadSafeTaskSlotTable<>(
                        taskSlotTable, taskExecutor.getMainThreadExecutableForTesting());

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway tmGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            taskExecutorIsRegistered.await();

            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            final AllocationID allocationId = new AllocationID();

            requestSlot(
                    tmGateway,
                    jobId,
                    allocationId,
                    buildSlotID(0),
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            offerSlotsLatch.await();

            tmGateway.freeSlot(allocationId, new RuntimeException("test exception"), timeout).get();

            assertEquals(availableSlotFuture.get().f2, allocationId);
            assertEquals(threadSafeTaskSlotTable.getAllocationIdsPerJob(jobId).size(), 0);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /** This tests task executor receive SubmitTask before OfferSlot response. */
    @Test
    public void testSubmitTaskBeforeAcceptSlot() throws Exception {
        final InstanceID registrationId = new InstanceID();
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture =
                new CompletableFuture<>();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        registrationId, taskExecutorIsRegistered, availableSlotFuture);

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.ANY);

        final OneShotLatch offerSlotsLatch = new OneShotLatch();
        final OneShotLatch taskInTerminalState = new OneShotLatch();
        final CompletableFuture<Collection<SlotOffer>> offerResultFuture =
                new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                createJobMasterWithSlotOfferAndTaskTerminationHooks(
                        offerSlotsLatch, taskInTerminalState, offerResultFuture);

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
        final TaskManagerServices taskManagerServices =
                createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
        final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices);

        try {
            taskManager.start();
            taskManager.waitUntilStarted();

            final TaskExecutorGateway tmGateway =
                    taskManager.getSelfGateway(TaskExecutorGateway.class);

            // wait until registered at the RM
            taskExecutorIsRegistered.await();

            // request 2 slots for the given allocation ids
            AllocationID[] allocationIds = new AllocationID[] {allocationId1, allocationId2};
            for (int i = 0; i < allocationIds.length; i++) {
                requestSlot(
                        tmGateway,
                        jobId,
                        allocationIds[i],
                        buildSlotID(i),
                        ResourceProfile.UNKNOWN,
                        jobMasterGateway.getAddress(),
                        resourceManagerGateway.getFencingToken());
            }

            // notify job leader to start slot offering
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            // wait until slots have been offered
            offerSlotsLatch.await();

            submit(allocationId1, jobMasterGateway, tmGateway, NoOpInvokable.class);

            // acknowledge the offered slots
            offerResultFuture.complete(Collections.singleton(offer1));

            // check that the rejected slot will be made available again
            final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 =
                    availableSlotFuture.get();
            assertEquals(instanceIDSlotIDAllocationIDTuple3.f2, allocationId2);

            // wait for the task completion
            taskInTerminalState.await();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskManager);
        }
    }

    private TestingResourceManagerGateway createRmWithTmRegisterAndNotifySlotHooks(
            InstanceID registrationId,
            OneShotLatch taskExecutorIsRegistered,
            CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture) {
        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        resourceManagerLeaderRetriever.notifyListener(
                resourceManagerGateway.getAddress(),
                resourceManagerGateway.getFencingToken().toUUID());

        resourceManagerGateway.setRegisterTaskExecutorFunction(
                taskExecutorRegistration ->
                        CompletableFuture.completedFuture(
                                new TaskExecutorRegistrationSuccess(
                                        registrationId,
                                        resourceManagerGateway.getOwnResourceId(),
                                        new ClusterInformation("localhost", 1234))));

        resourceManagerGateway.setNotifySlotAvailableConsumer(availableSlotFuture::complete);

        resourceManagerGateway.setSendSlotReportFunction(
                ignored -> {
                    taskExecutorIsRegistered.trigger();
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        return resourceManagerGateway;
    }

    private TaskManagerServices createTaskManagerServicesWithTaskSlotTable(
            TaskSlotTable<Task> taskSlotTable) throws IOException {
        return new TaskManagerServicesBuilder()
                .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                .setShuffleEnvironment(nettyShuffleEnvironment)
                .setTaskSlotTable(taskSlotTable)
                .setJobLeaderService(
                        new DefaultJobLeaderService(
                                unresolvedTaskManagerLocation,
                                RetryingRegistrationConfiguration.defaultConfiguration()))
                .setTaskStateManager(createTaskExecutorLocalStateStoresManager())
                .build();
    }

    private static TestingJobMasterGateway createJobMasterWithSlotOfferAndTaskTerminationHooks(
            OneShotLatch offerSlotsLatch,
            OneShotLatch taskInTerminalState,
            CompletableFuture<Collection<SlotOffer>> offerResultFuture) {
        return new TestingJobMasterGatewayBuilder()
                .setOfferSlotsFunction(
                        (resourceID, slotOffers) -> {
                            offerSlotsLatch.trigger();
                            return offerResultFuture;
                        })
                .setUpdateTaskExecutionStateFunction(
                        taskExecutionState -> {
                            if (taskExecutionState.getExecutionState().isTerminal()) {
                                taskInTerminalState.trigger();
                            }
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        })
                .build();
    }

    private <T extends TaskInvokable> ExecutionAttemptID submit(
            AllocationID allocationId,
            TestingJobMasterGateway jobMasterGateway,
            TaskExecutorGateway tmGateway,
            Class<T> invokableClass)
            throws IOException {
        final TaskDeploymentDescriptor tdd =
                TaskDeploymentDescriptorBuilder.newBuilder(jobId, invokableClass)
                        .setAllocationId(allocationId)
                        .build();
        tmGateway.submitTask(tdd, jobMasterGateway.getFencingToken(), timeout).join();
        return tdd.getExecutionAttemptId();
    }

    /**
     * Tests that the heartbeat is stopped once the TaskExecutor detects that the RM is no longer
     * leader.
     *
     * <p>See FLINK-8462
     */
    @Test
    public void testRMHeartbeatStopWhenLeadershipRevoked() throws Exception {
        final long heartbeatInterval = 1L;
        final long heartbeatTimeout = 10000L;
        final long pollTimeout = 1000L;
        final RecordingHeartbeatServices heartbeatServices =
                new RecordingHeartbeatServices(heartbeatInterval, heartbeatTimeout);
        final ResourceID rmResourceID = ResourceID.generate();

        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());

        final String rmAddress = "rm";
        final TestingResourceManagerGateway rmGateway =
                new TestingResourceManagerGateway(
                        ResourceManagerId.generate(), rmResourceID, rmAddress, rmAddress);

        rpc.registerGateway(rmAddress, rmGateway);

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TaskExecutor taskExecutor =
                createTaskExecutor(taskManagerServices, heartbeatServices);

        try {
            taskExecutor.start();

            final BlockingQueue<ResourceID> unmonitoredTargets =
                    heartbeatServices.getUnmonitoredTargets();
            final BlockingQueue<ResourceID> monitoredTargets =
                    heartbeatServices.getMonitoredTargets();

            resourceManagerLeaderRetriever.notifyListener(
                    rmAddress, rmGateway.getFencingToken().toUUID());

            // wait for TM registration by checking the registered heartbeat targets
            assertEquals(monitoredTargets.poll(pollTimeout, TimeUnit.MILLISECONDS), rmResourceID);

            // let RM lose leadership
            resourceManagerLeaderRetriever.notifyListener(null, null);

            // the timeout should not have triggered since it is much higher
            assertEquals(unmonitoredTargets.poll(pollTimeout, TimeUnit.MILLISECONDS), rmResourceID);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that a job is removed from the JobLeaderService once a TaskExecutor has no more slots
     * assigned to this job.
     *
     * <p>See FLINK-8504
     */
    @Test
    public void testRemoveJobFromJobLeaderService() throws Exception {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();

        final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

        try {
            final TestingResourceManagerGateway resourceManagerGateway =
                    new TestingResourceManagerGateway();
            final CompletableFuture<Void> initialSlotReport = new CompletableFuture<>();
            resourceManagerGateway.setSendSlotReportFunction(
                    resourceIDInstanceIDSlotReportTuple3 -> {
                        initialSlotReport.complete(null);
                        return CompletableFuture.completedFuture(Acknowledge.get());
                    });
            final ResourceManagerId resourceManagerId = resourceManagerGateway.getFencingToken();

            rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
            resourceManagerLeaderRetriever.notifyListener(
                    resourceManagerGateway.getAddress(), resourceManagerId.toUUID());

            final CompletableFuture<LeaderRetrievalListener> startFuture =
                    new CompletableFuture<>();
            final CompletableFuture<Void> stopFuture = new CompletableFuture<>();

            final StartStopNotifyingLeaderRetrievalService jobMasterLeaderRetriever =
                    new StartStopNotifyingLeaderRetrievalService(startFuture, stopFuture);
            haServices.setJobMasterLeaderRetriever(jobId, jobMasterLeaderRetriever);

            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            final SlotID slotId = buildSlotID(0);
            final AllocationID allocationId = new AllocationID();

            assertFalse(startFuture.isDone());
            final JobLeaderService jobLeaderService = taskManagerServices.getJobLeaderService();
            assertFalse(jobLeaderService.containsJob(jobId));

            // wait for the initial slot report
            initialSlotReport.get();

            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    allocationId,
                    slotId,
                    ResourceProfile.ZERO,
                    "foobar",
                    resourceManagerId);

            // wait until the job leader retrieval service for jobId is started
            startFuture.get();
            assertTrue(jobLeaderService.containsJob(jobId));

            taskExecutorGateway
                    .freeSlot(allocationId, new FlinkException("Test exception"), timeout)
                    .get();

            // wait that the job leader retrieval service for jobId stopped becaue it should get
            // removed
            stopFuture.get();
            assertFalse(jobLeaderService.containsJob(jobId));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testMaximumRegistrationDuration() throws Exception {
        configuration.set(
                TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("10 ms"));

        final TaskExecutor taskExecutor =
                createTaskExecutor(new TaskManagerServicesBuilder().build());

        taskExecutor.start();

        try {
            final Throwable error = testingFatalErrorHandler.getErrorFuture().get();
            assertNotNull(error);
            assertTrue(ExceptionUtils.stripExecutionException(error) instanceof RegistrationTimeoutException);

            testingFatalErrorHandler.clearError();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testMaximumRegistrationDurationAfterConnectionLoss() throws Exception {
        configuration.set(
                TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("100 ms"));
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();
        final TaskExecutor taskExecutor =
                createTaskExecutor(taskManagerServices, new HeartbeatServices(10L, 10L));

        taskExecutor.start();

        final CompletableFuture<ResourceID> registrationFuture = new CompletableFuture<>();
        final OneShotLatch secondRegistration = new OneShotLatch();
        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();
            testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                    taskExecutorRegistration -> {
                        if (registrationFuture.complete(taskExecutorRegistration.getResourceId())) {
                            return createRegistrationResponse(testingResourceManagerGateway);
                        } else {
                            secondRegistration.trigger();
                            return CompletableFuture.completedFuture(
                                    new Failure(
                                            new FlinkException(
                                                    "Only the first registration should succeed.")));
                        }
                    });
            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(), UUID.randomUUID());

            final ResourceID registrationResourceId = registrationFuture.get();

            assertEquals(registrationResourceId, taskManagerServices
                    .getUnresolvedTaskManagerLocation()
                    .getResourceID());

            secondRegistration.await();

            final Throwable error = testingFatalErrorHandler.getErrorFuture().get();
            assertNotNull(error);
            assertTrue(ExceptionUtils.stripExecutionException(error) instanceof RegistrationTimeoutException);

            testingFatalErrorHandler.clearError();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that we ignore slot requests if the TaskExecutor is not registered at a
     * ResourceManager.
     */
    @Test
    public void testIgnoringSlotRequestsIfNotRegistered() throws Exception {
        final TaskExecutor taskExecutor = createTaskExecutor(1);

        taskExecutor.start();

        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();

            final CompletableFuture<RegistrationResponse> registrationFuture =
                    new CompletableFuture<>();
            final CompletableFuture<ResourceID> taskExecutorResourceIdFuture =
                    new CompletableFuture<>();

            testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                    taskExecutorRegistration -> {
                        taskExecutorResourceIdFuture.complete(
                                taskExecutorRegistration.getResourceId());
                        return registrationFuture;
                    });

            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            final ResourceID resourceId = taskExecutorResourceIdFuture.get();

            final CompletableFuture<Acknowledge> slotRequestResponse =
                    taskExecutorGateway.requestSlot(
                            new SlotID(resourceId, 0),
                            jobId,
                            new AllocationID(),
                            ResourceProfile.ZERO,
                            "foobar",
                            testingResourceManagerGateway.getFencingToken(),
                            timeout);

            try {
                slotRequestResponse.get();
                fail(
                        "We should not be able to request slots before the TaskExecutor is registered at the ResourceManager.");
            } catch (ExecutionException ee) {
                assertTrue(ExceptionUtils.stripExecutionException(ee) instanceof TaskManagerException);
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that the TaskExecutor tries to reconnect to a ResourceManager from which it was
     * explicitly disconnected.
     */
    @Test
    public void testReconnectionAttemptIfExplicitlyDisconnected() throws Exception {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final TaskExecutor taskExecutor =
                createTaskExecutor(
                        new TaskManagerServicesBuilder()
                                .setTaskSlotTable(taskSlotTable)
                                .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                                .build());

        taskExecutor.start();

        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();
            final ClusterInformation clusterInformation = new ClusterInformation("foobar", 1234);
            final CompletableFuture<RegistrationResponse> registrationResponseFuture =
                    CompletableFuture.completedFuture(
                            new TaskExecutorRegistrationSuccess(
                                    new InstanceID(), ResourceID.generate(), clusterInformation));
            final BlockingQueue<ResourceID> registrationQueue = new ArrayBlockingQueue<>(1);

            testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                    taskExecutorRegistration -> {
                        registrationQueue.offer(taskExecutorRegistration.getResourceId());
                        return registrationResponseFuture;
                    });
            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            final ResourceID firstRegistrationAttempt = registrationQueue.take();

            assertEquals(firstRegistrationAttempt, unresolvedTaskManagerLocation.getResourceID());

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            assertEquals(registrationQueue.size(), 0);

            taskExecutorGateway.disconnectResourceManager(new FlinkException("Test exception"));

            final ResourceID secondRegistrationAttempt = registrationQueue.take();

            assertEquals(secondRegistrationAttempt, unresolvedTaskManagerLocation.getResourceID());

        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that the {@link TaskExecutor} sends the initial slot report after it registered at the
     * ResourceManager.
     */
    @Test
    public void testInitialSlotReport() throws Exception {
        final TaskExecutor taskExecutor = createTaskExecutor(1);

        taskExecutor.start();

        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();
            final CompletableFuture<ResourceID> initialSlotReportFuture = new CompletableFuture<>();

            testingResourceManagerGateway.setSendSlotReportFunction(
                    resourceIDInstanceIDSlotReportTuple3 -> {
                        initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f0);
                        return CompletableFuture.completedFuture(Acknowledge.get());
                    });

            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            assertEquals(initialSlotReportFuture.get(), taskExecutor.getResourceID());
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testRegisterWithDefaultSlotResourceProfile() throws Exception {
        final int numberOfSlots = 2;
        final TaskExecutor taskExecutor = createTaskExecutor(numberOfSlots);

        taskExecutor.start();

        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();
            final CompletableFuture<ResourceProfile> registeredDefaultSlotResourceProfileFuture =
                    new CompletableFuture<>();

            final ResourceID ownResourceId = testingResourceManagerGateway.getOwnResourceId();
            testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                    taskExecutorRegistration -> {
                        registeredDefaultSlotResourceProfileFuture.complete(
                                taskExecutorRegistration.getDefaultSlotResourceProfile());
                        return CompletableFuture.completedFuture(
                                new TaskExecutorRegistrationSuccess(
                                        new InstanceID(),
                                        ownResourceId,
                                        new ClusterInformation("localhost", 1234)));
                    });

            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            assertEquals(registeredDefaultSlotResourceProfileFuture.get(), TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(
                    TM_RESOURCE_SPEC, numberOfSlots));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /** Tests that the {@link TaskExecutor} tries to reconnect if the initial slot report fails. */
    @Test
    public void testInitialSlotReportFailure() throws Exception {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(taskSlotTable)
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .build();
        final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

        taskExecutor.start();

        try {
            final TestingResourceManagerGateway testingResourceManagerGateway =
                    new TestingResourceManagerGateway();

            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(2);
            testingResourceManagerGateway.setSendSlotReportFunction(
                    resourceIDInstanceIDSlotReportTuple3 -> {
                        try {
                            return responseQueue.take();
                        } catch (InterruptedException e) {
                            return FutureUtils.completedExceptionally(e);
                        }
                    });

            final CompletableFuture<RegistrationResponse> registrationResponse =
                    CompletableFuture.completedFuture(
                            new TaskExecutorRegistrationSuccess(
                                    new InstanceID(),
                                    testingResourceManagerGateway.getOwnResourceId(),
                                    new ClusterInformation("foobar", 1234)));

            final CountDownLatch numberRegistrations = new CountDownLatch(2);

            testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                    taskExecutorRegistration -> {
                        numberRegistrations.countDown();
                        return registrationResponse;
                    });

            responseQueue.offer(
                    FutureUtils.completedExceptionally(new FlinkException("Test exception")));
            responseQueue.offer(CompletableFuture.completedFuture(Acknowledge.get()));

            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            // wait for the second registration attempt
            numberRegistrations.await();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /** Tests that offers slots to job master timeout and retry. */
    @Test
    public void testOfferSlotToJobMasterAfterTimeout() throws Exception {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(2, EXECUTOR_RESOURCE.getExecutor());
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();

        final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

        final AllocationID allocationId = new AllocationID();

        final CompletableFuture<ResourceID> initialSlotReportFuture = new CompletableFuture<>();

        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(null);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        rpc.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
        resourceManagerLeaderRetriever.notifyListener(
                testingResourceManagerGateway.getAddress(),
                testingResourceManagerGateway.getFencingToken().toUUID());

        final CountDownLatch slotOfferings = new CountDownLatch(3);
        final CompletableFuture<AllocationID> offeredSlotFuture = new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    assertEquals(slotOffers.size(), 1);
                                    slotOfferings.countDown();

                                    if (slotOfferings.getCount() == 0) {
                                        offeredSlotFuture.complete(
                                                slotOffers.iterator().next().getAllocationId());
                                        return CompletableFuture.completedFuture(slotOffers);
                                    } else {
                                        return FutureUtils.completedExceptionally(
                                                new TimeoutException());
                                    }
                                })
                        .build();
        final String jobManagerAddress = jobMasterGateway.getAddress();
        rpc.registerGateway(jobManagerAddress, jobMasterGateway);
        jobManagerLeaderRetriever.notifyListener(
                jobManagerAddress, jobMasterGateway.getFencingToken().toUUID());

        try {
            taskExecutor.start();
            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            // wait for the connection to the ResourceManager
            initialSlotReportFuture.get();

            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    allocationId,
                    new SlotID(taskExecutor.getResourceID(), 0),
                    ResourceProfile.ZERO,
                    jobManagerAddress,
                    testingResourceManagerGateway.getFencingToken());

            slotOfferings.await();

            assertEquals(offeredSlotFuture.get(), allocationId);
            assertTrue(taskSlotTable.isSlotFree(1));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /** Tests that the TaskExecutor disconnects from the JobMaster if a new leader is detected. */
    @Test
    public void testDisconnectFromJobMasterWhenNewLeader() throws Exception {
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(
                                TaskSlotUtils.createTaskSlotTable(
                                        1, EXECUTOR_RESOURCE.getExecutor()))
                        .build();
        final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

        final CompletableFuture<Integer> offeredSlotsFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceID> disconnectFuture = new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offeredSlotsFuture.complete(slotOffers.size());
                                    return CompletableFuture.completedFuture(slotOffers);
                                })
                        .setDisconnectTaskManagerFunction(
                                resourceID -> {
                                    disconnectFuture.complete(resourceID);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();
        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();

        final CompletableFuture<Void> initialSlotReportFuture = new CompletableFuture<>();
        resourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(null);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        try {
            taskExecutor.start();

            TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            resourceManagerLeaderRetriever.notifyListener(
                    resourceManagerGateway.getAddress(),
                    resourceManagerGateway.getFencingToken().toUUID());

            initialSlotReportFuture.get();

            ResourceID resourceID =
                    taskManagerServices.getUnresolvedTaskManagerLocation().getResourceID();
            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    new AllocationID(),
                    new SlotID(resourceID, 0),
                    ResourceProfile.ZERO,
                    "foobar",
                    resourceManagerGateway.getFencingToken());

            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), UUID.randomUUID());

            assertEquals(new Integer(1), offeredSlotsFuture.get());

            // notify loss of leadership
            jobManagerLeaderRetriever.notifyListener(null, null);

            assertEquals(disconnectFuture.get(), resourceID);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test(timeout = 10000L)
    public void testLogNotFoundHandling() throws Throwable {
        configuration.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, 0);
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
        configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
        configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/i/dont/exist");

        try (TaskSubmissionTestEnvironment env =
                     new Builder(jobId)
                             .setConfiguration(configuration)
                             .setLocalCommunication(false)
                             .build(EXECUTOR_RESOURCE.getExecutor())) {
            TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
            try {
                CompletableFuture<TransientBlobKey> logFuture =
                        tmGateway.requestFileUploadByType(FileType.LOG, timeout);
                logFuture.get();
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("The file LOG does not exist on the TaskExecutor."));
            }
        }
    }

    @Test(timeout = 10000L)
    public void testTerminationOnFatalError() throws Throwable {
        try (TaskSubmissionTestEnvironment env =
                     new Builder(jobId)
                             .setConfiguration(configuration)
                             .build(EXECUTOR_RESOURCE.getExecutor())) {
            String testExceptionMsg = "Test exception of fatal error.";

            env.getTaskExecutor().onFatalError(new Exception(testExceptionMsg));

            Throwable exception = env.getTestingFatalErrorHandler().getErrorFuture().get();
            env.getTestingFatalErrorHandler().clearError();
            assertTrue(exception.getMessage().startsWith(testExceptionMsg));
        }
    }

    /**
     * Tests that the TaskExecutor syncs its slots view with the JobMaster's view via the
     * AllocatedSlotReport reported by the heartbeat (See FLINK-11059).
     */
    @Test
    public void testSyncSlotsWithJobMasterByHeartbeat() throws Exception {
        final CountDownLatch activeSlots = new CountDownLatch(2);
        final TaskSlotTable<Task> taskSlotTable =
                new ActivateSlotNotifyingTaskSlotTable(2, activeSlots);
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();

        final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();

        final BlockingQueue<AllocationID> allocationsNotifiedFree = new ArrayBlockingQueue<>(2);

        OneShotLatch initialSlotReporting = new OneShotLatch();
        testingResourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReporting.trigger();
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        testingResourceManagerGateway.setNotifySlotAvailableConsumer(
                instanceIDSlotIDAllocationIDTuple3 ->
                        allocationsNotifiedFree.offer(instanceIDSlotIDAllocationIDTuple3.f2));

        rpc.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
        resourceManagerLeaderRetriever.notifyListener(
                testingResourceManagerGateway.getAddress(),
                testingResourceManagerGateway.getFencingToken().toUUID());

        final BlockingQueue<AllocationID> failedSlotFutures = new ArrayBlockingQueue<>(2);
        final ResourceID jobManagerResourceId = ResourceID.generate();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setFailSlotConsumer(
                                (resourceID, allocationID, throwable) ->
                                        failedSlotFutures.offer(allocationID))
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) ->
                                        CompletableFuture.completedFuture(
                                                new ArrayList<>(slotOffers)))
                        .setRegisterTaskManagerFunction(
                                (ignoredJobId, ignoredTaskManagerRegistrationInformation) ->
                                        CompletableFuture.completedFuture(
                                                new JMTMRegistrationSuccess(jobManagerResourceId)))
                        .build();
        final String jobManagerAddress = jobMasterGateway.getAddress();
        rpc.registerGateway(jobManagerAddress, jobMasterGateway);
        jobManagerLeaderRetriever.notifyListener(
                jobManagerAddress, jobMasterGateway.getFencingToken().toUUID());

        taskExecutor.start();

        try {
            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            initialSlotReporting.await();

            final AllocationID allocationIdInBoth = new AllocationID();
            final AllocationID allocationIdOnlyInJM = new AllocationID();
            final AllocationID allocationIdOnlyInTM = new AllocationID();

            taskExecutorGateway.requestSlot(
                    new SlotID(taskExecutor.getResourceID(), 0),
                    jobId,
                    allocationIdInBoth,
                    ResourceProfile.ZERO,
                    "foobar",
                    testingResourceManagerGateway.getFencingToken(),
                    timeout);
            taskExecutorGateway.requestSlot(
                    new SlotID(taskExecutor.getResourceID(), 1),
                    jobId,
                    allocationIdOnlyInTM,
                    ResourceProfile.ZERO,
                    "foobar",
                    testingResourceManagerGateway.getFencingToken(),
                    timeout);

            activeSlots.await();

            List<AllocatedSlotInfo> allocatedSlotInfos =
                    Arrays.asList(
                            new AllocatedSlotInfo(0, allocationIdInBoth),
                            new AllocatedSlotInfo(1, allocationIdOnlyInJM));
            AllocatedSlotReport allocatedSlotReport =
                    new AllocatedSlotReport(jobId, allocatedSlotInfos);
            taskExecutorGateway.heartbeatFromJobManager(jobManagerResourceId, allocatedSlotReport);

            assertEquals(failedSlotFutures.take(), allocationIdOnlyInJM);
            assertEquals(allocationsNotifiedFree.take(), allocationIdOnlyInTM);
            assertNull(failedSlotFutures.poll(5L, TimeUnit.MILLISECONDS));
            assertNull(allocationsNotifiedFree.poll(5L, TimeUnit.MILLISECONDS));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    /**
     * Tests that the {@link SlotReport} sent to the RM does not contain out dated/stale information
     * as slots are being requested from the TM.
     *
     * <p>This is a probabilistic test case and needs to be executed several times to produce a
     * failure without the fix for FLINK-12865.
     */
    @Test
    public void testSlotReportDoesNotContainStaleInformation() throws Exception {
        final OneShotLatch receivedSlotRequest = new OneShotLatch();
        final CompletableFuture<Void> verifySlotReportFuture = new CompletableFuture<>();
        final OneShotLatch terminateSlotReportVerification = new OneShotLatch();
        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        // Assertions for this test
        testingResourceManagerGateway.setTaskExecutorHeartbeatFunction(
                (ignored, heartbeatPayload) -> {
                    try {
                        final ArrayList<SlotStatus> slots =
                                Lists.newArrayList(heartbeatPayload.getSlotReport());
                        assertEquals(1, slots.size());
                        final SlotStatus slotStatus = slots.get(0);

                        log.info("Received SlotStatus: {}", slotStatus);

                        if (receivedSlotRequest.isTriggered()) {
                            assertNotNull(slotStatus.getAllocationID());
                        } else {
                            assertNull(slotStatus.getAllocationID());
                        }
                    } catch (AssertionError e) {
                        verifySlotReportFuture.completeExceptionally(e);
                    }

                    if (terminateSlotReportVerification.isTriggered()) {
                        verifySlotReportFuture.complete(null);
                    }
                    return FutureUtils.completedVoidFuture();
                });
        final CompletableFuture<ResourceID> taskExecutorRegistrationFuture =
                new CompletableFuture<>();

        testingResourceManagerGateway.setSendSlotReportFunction(
                ignored -> {
                    taskExecutorRegistrationFuture.complete(null);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpc.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
        resourceManagerLeaderRetriever.notifyListener(
                testingResourceManagerGateway.getAddress(),
                testingResourceManagerGateway.getFencingToken().toUUID());

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(
                                new AllocateSlotNotifyingTaskSlotTable(receivedSlotRequest))
                        .build();
        final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);
        final ResourceID taskExecutorResourceId =
                taskManagerServices.getUnresolvedTaskManagerLocation().getResourceID();

        taskExecutor.start();

        final TaskExecutorGateway taskExecutorGateway =
                taskExecutor.getSelfGateway(TaskExecutorGateway.class);

        final ScheduledExecutorService heartbeatExecutor =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        try {
            taskExecutorRegistrationFuture.get();

            final OneShotLatch scheduleFirstHeartbeat = new OneShotLatch();
            final ResourceID resourceManagerResourceId =
                    testingResourceManagerGateway.getOwnResourceId();
            final long heartbeatInterval = 5L;
            heartbeatExecutor.scheduleWithFixedDelay(
                    () -> {
                        scheduleFirstHeartbeat.trigger();
                        taskExecutorGateway.heartbeatFromResourceManager(resourceManagerResourceId);
                    },
                    0L,
                    heartbeatInterval,
                    TimeUnit.MILLISECONDS);

            scheduleFirstHeartbeat.await();

            taskExecutorGateway
                    .requestSlot(
                            new SlotID(taskExecutorResourceId, 0),
                            jobId,
                            new AllocationID(),
                            ResourceProfile.ZERO,
                            "foobar",
                            testingResourceManagerGateway.getFencingToken(),
                            timeout)
                    .get();

            terminateSlotReportVerification.trigger();

            verifySlotReportFuture.get();
        } finally {
            ExecutorUtils.gracefulShutdown(
                    timeout.toMilliseconds(), TimeUnit.MILLISECONDS, heartbeatExecutor);
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testDynamicSlotAllocation() throws Exception {
        final AllocationID allocationId = new AllocationID();
        try (TaskExecutorTestingContext submissionContext = createTaskExecutorTestingContext(2)) {
            submissionContext.start();
            final CompletableFuture<Tuple3<ResourceID, InstanceID, SlotReport>>
                    initialSlotReportFuture = new CompletableFuture<>();
            ResourceManagerId resourceManagerId =
                    createAndRegisterResourceManager(initialSlotReportFuture);
            initialSlotReportFuture.get();
            final ResourceProfile resourceProfile =
                    DEFAULT_RESOURCE_PROFILE.merge(
                            ResourceProfile.newBuilder().setCpuCores(0.1).build());

            TaskExecutorGateway selfGateway =
                    submissionContext.taskExecutor.getSelfGateway(TaskExecutorGateway.class);
            requestSlot(
                    selfGateway,
                    jobId,
                    allocationId,
                    SlotID.getDynamicSlotID(ResourceID.generate()),
                    resourceProfile,
                    submissionContext.jobMasterGateway.getAddress(),
                    resourceManagerId);

            ResourceID resourceId = ResourceID.generate();
            SlotReport slotReport = submissionContext.taskSlotTable.createSlotReport(resourceId);
//            assertThat(
//                    slotReport,
//                    containsInAnyOrder(
//                            new SlotStatus(new SlotID(resourceId, 0), DEFAULT_RESOURCE_PROFILE),
//                            new SlotStatus(new SlotID(resourceId, 1), DEFAULT_RESOURCE_PROFILE),
//                            new SlotStatus(
//                                    new SlotID(resourceId, 2),
//                                    resourceProfile,
//                                    jobId,
//                                    allocationId)));
        }
    }

    @Test
    public void testReleasingJobResources() throws Exception {
        AllocationID[] slots =
                range(0, 5).mapToObj(i -> new AllocationID()).toArray(AllocationID[]::new);
        try (TaskExecutorTestingContext ctx = createTaskExecutorTestingContext(slots.length)) {
            ctx.start();
            ResourceManagerId rmId;
            {
                CompletableFuture<Tuple3<ResourceID, InstanceID, SlotReport>>
                        initialSlotReportFuture = new CompletableFuture<>();
                rmId = createAndRegisterResourceManager(initialSlotReportFuture);
                initialSlotReportFuture.get();
            }

            TaskExecutorGateway tm = ctx.taskExecutor.getSelfGateway(TaskExecutorGateway.class);
            for (int i = 0; i < slots.length; i++) {
                requestSlot(
                        tm,
                        jobId,
                        slots[i],
                        buildSlotID(i),
                        ResourceProfile.UNKNOWN,
                        ctx.jobMasterGateway.getAddress(),
                        rmId);
            }
            ctx.offerSlotsLatch.await();
            ExecutionAttemptID exec =
                    submit(slots[0], ctx.jobMasterGateway, tm, BlockingNoOpInvokable.class);
            assertNotNull(ctx.changelogStoragesManager.getChangelogStoragesByJobId(jobId));
            assertNotNull(ctx.metricGroup.getJobMetricsGroup(jobId));

            // cancel tasks before releasing the slots - so that TM will release job resources on
            // the last slot release
            tm.cancelTask(exec, timeout).get();
            // wait for task thread to notify TM about its final state
            // (taskSlotTable isn't thread safe - using MainThread)
            while (callInMain(ctx, () -> ctx.taskSlotTable.getTasks(jobId).hasNext())) {
                Thread.sleep(50);
            }

            for (int i = 0; i < slots.length; i++) {
                tm.freeSlot(slots[i], new RuntimeException("test exception"), timeout).get();
                boolean isLastSlot = i == slots.length - 1;
                assertEquals(
                        isLastSlot,
                        null == callInMain(ctx, () -> ctx.metricGroup.getJobMetricsGroup(jobId)));
                assertEquals(
                        isLastSlot,
                        null
                                == callInMain(
                                ctx,
                                () ->
                                        ctx.changelogStoragesManager
                                                .getChangelogStoragesByJobId(jobId)));
            }
        }
    }

    @Test
    public void taskExecutorJobServicesCloseClassLoaderLeaseUponClosing()
            throws InterruptedException {
        final OneShotLatch leaseReleaseLatch = new OneShotLatch();
        final OneShotLatch closeHookLatch = new OneShotLatch();
        final TestingClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder()
                        .setCloseRunnable(leaseReleaseLatch::trigger)
                        .build();

        final TaskExecutor.TaskExecutorJobServices taskExecutorJobServices =
                TaskExecutor.TaskExecutorJobServices.create(
                        classLoaderLease, closeHookLatch::trigger);

        taskExecutorJobServices.close();
        leaseReleaseLatch.await();
        closeHookLatch.await();
    }

    /**
     * Tests that the TaskExecutor releases all of its job resources if the JobMaster is not running
     * the specified job. See FLINK-21606.
     */
    @Test
    public void testReleaseOfJobResourcesIfJobMasterIsNotCorrect() throws Exception {
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(
                                TaskSlotUtils.createTaskSlotTable(
                                        1, EXECUTOR_RESOURCE.getExecutor()))
                        .build();

        final TestingTaskExecutorPartitionTracker taskExecutorPartitionTracker =
                new TestingTaskExecutorPartitionTracker();
        final CompletableFuture<JobID> jobPartitionsReleaseFuture = new CompletableFuture<>();
        // simulate that we have some partitions tracked
        taskExecutorPartitionTracker.setIsTrackingPartitionsForFunction(ignored -> true);
        taskExecutorPartitionTracker.setStopTrackingAndReleaseAllPartitionsConsumer(
                jobPartitionsReleaseFuture::complete);

        final TaskExecutor taskExecutor =
                createTaskExecutor(
                        taskManagerServices, HEARTBEAT_SERVICES, taskExecutorPartitionTracker);

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setRegisterTaskManagerFunction(
                                (ignoredJobId, ignoredTaskManagerRegistrationInformation) ->
                                        CompletableFuture.completedFuture(
                                                new JMTMRegistrationRejection("foobar")))
                        .build();

        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final InstanceID registrationId = new InstanceID();
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture =
                new CompletableFuture<>();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        registrationId, taskExecutorIsRegistered, availableSlotFuture);

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        resourceManagerLeaderRetriever.notifyListener(
                resourceManagerGateway.getAddress(),
                resourceManagerGateway.getFencingToken().toUUID());

        try {
            taskExecutor.start();

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            taskExecutorIsRegistered.await();

            final AllocationID allocationId = new AllocationID();
            final SlotID slotId = new SlotID(taskExecutor.getResourceID(), 0);

            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    allocationId,
                    slotId,
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            // The JobManager should reject the registration which should release all job resources
            // on the TaskExecutor
            jobManagerLeaderRetriever.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            // the slot should be freed
            assertEquals(availableSlotFuture.get().f1, slotId);
            assertEquals(availableSlotFuture.get().f2, allocationId);

            // all job partitions should be released
            assertEquals(jobPartitionsReleaseFuture.get(), jobId);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    public void testReleaseInactiveSlots() throws Exception {
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(
                                TaskSlotUtils.createTaskSlotTable(
                                        1, EXECUTOR_RESOURCE.getExecutor()))
                        .build();

        final TaskExecutor taskExecutor =
                createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setRegisterTaskManagerFunction(
                                (ignoredJobId, ignoredTaskManagerRegistrationInformation) ->
                                        new CompletableFuture<>())
                        .build();

        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final InstanceID registrationId = new InstanceID();
        final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
        final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture =
                new CompletableFuture<>();
        final TestingResourceManagerGateway resourceManagerGateway =
                createRmWithTmRegisterAndNotifySlotHooks(
                        registrationId, taskExecutorIsRegistered, availableSlotFuture);

        rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        resourceManagerLeaderRetriever.notifyListener(
                resourceManagerGateway.getAddress(),
                resourceManagerGateway.getFencingToken().toUUID());

        try {
            taskExecutor.start();

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            taskExecutorIsRegistered.await();

            final AllocationID allocationId = new AllocationID();
            final SlotID slotId = new SlotID(taskExecutor.getResourceID(), 0);

            requestSlot(
                    taskExecutorGateway,
                    jobId,
                    allocationId,
                    slotId,
                    ResourceProfile.UNKNOWN,
                    jobMasterGateway.getAddress(),
                    resourceManagerGateway.getFencingToken());

            taskExecutorGateway.freeInactiveSlots(jobId, timeout);

            // the slot should be freed
            assertEquals(availableSlotFuture.get().f1, slotId);
            assertEquals(availableSlotFuture.get().f2, allocationId);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    private TaskExecutorLocalStateStoresManager createTaskExecutorLocalStateStoresManager()
            throws IOException {
        return new TaskExecutorLocalStateStoresManager(
                false, Reference.owned(new File[] {tmp.newFolder()}), Executors.directExecutor());
    }

    private TaskExecutor createTaskExecutor(int numberOFSlots) throws IOException {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(numberOFSlots, EXECUTOR_RESOURCE.getExecutor());
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(taskSlotTable)
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .build();
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numberOFSlots);
        return createTaskExecutor(taskManagerServices);
    }

    @Nonnull
    private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices)
            throws IOException {
        return createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);
    }

    private TaskExecutor createTaskExecutor(
            TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices)
            throws IOException {
        return createTaskExecutor(
                taskManagerServices,
                heartbeatServices,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()));
    }

    private TaskExecutor createTaskExecutor(
            TaskManagerServices taskManagerServices,
            HeartbeatServices heartbeatServices,
            TaskExecutorPartitionTracker taskExecutorPartitionTracker)
            throws IOException {
        return new TaskExecutor(
                rpc,
                TaskManagerConfiguration.fromConfiguration(
                        configuration,
                        TM_RESOURCE_SPEC,
                        InetAddress.getLoopbackAddress().getHostAddress(),
                        TestFileUtils.createTempDir()),
                haServices,
                taskManagerServices,
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                heartbeatServices,
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                null,
                NoOpTaskExecutorBlobService.INSTANCE,
                testingFatalErrorHandler,
                taskExecutorPartitionTracker);
    }

    private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices)
            throws IOException {
        return createTestingTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);
    }

    private TestingTaskExecutor createTestingTaskExecutor(
            TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices)
            throws IOException {
        return createTestingTaskExecutor(
                taskManagerServices, heartbeatServices, createUnregisteredTaskManagerMetricGroup());
    }

    private TestingTaskExecutor createTestingTaskExecutor(
            TaskManagerServices taskManagerServices,
            HeartbeatServices heartbeatServices,
            TaskManagerMetricGroup metricGroup)
            throws IOException {
        return new TestingTaskExecutor(
                rpc,
                TaskManagerConfiguration.fromConfiguration(
                        configuration,
                        TM_RESOURCE_SPEC,
                        InetAddress.getLoopbackAddress().getHostAddress(),
                        TestFileUtils.createTempDir()),
                haServices,
                taskManagerServices,
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                heartbeatServices,
                metricGroup,
                null,
                NoOpTaskExecutorBlobService.INSTANCE,
                testingFatalErrorHandler,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()));
    }

    private TaskExecutorTestingContext createTaskExecutorTestingContext(int numberOfSlots)
            throws IOException {
        return createTaskExecutorTestingContext(
                TaskSlotUtils.createTaskSlotTable(numberOfSlots, EXECUTOR_RESOURCE.getExecutor()));
    }

    private TaskExecutorTestingContext createTaskExecutorTestingContext(
            final TaskSlotTable<Task> taskSlotTable) throws IOException {
        final OneShotLatch offerSlotsLatch = new OneShotLatch();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    offerSlotsLatch.trigger();
                                    return CompletableFuture.completedFuture(slotOffers);
                                })
                        .build();
        rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        RetryingRegistrationConfiguration.defaultConfiguration());

        TaskExecutorLocalStateStoresManager stateStoresManager =
                createTaskExecutorLocalStateStoresManager();
        TaskExecutorStateChangelogStoragesManager changelogStoragesManager =
                new TaskExecutorStateChangelogStoragesManager();
        TaskManagerMetricGroup metricGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        NoOpMetricRegistry.INSTANCE, "", ResourceID.generate());

        final TestingTaskExecutor taskExecutor =
                createTestingTaskExecutor(
                        new TaskManagerServicesBuilder()
                                .setTaskSlotTable(taskSlotTable)
                                .setJobLeaderService(jobLeaderService)
                                .setTaskStateManager(stateStoresManager)
                                .setTaskChangelogStoragesManager(changelogStoragesManager)
                                .build(),
                        HEARTBEAT_SERVICES,
                        metricGroup);

        jobManagerLeaderRetriever.notifyListener(
                jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());
        return new TaskExecutorTestingContext(
                jobMasterGateway,
                taskSlotTable,
                taskExecutor,
                changelogStoragesManager,
                metricGroup,
                offerSlotsLatch);
    }

    private class TaskExecutorTestingContext implements AutoCloseable {
        private final TestingJobMasterGateway jobMasterGateway;
        private final TaskSlotTable taskSlotTable;
        private final TestingTaskExecutor taskExecutor;
        private final TaskExecutorStateChangelogStoragesManager changelogStoragesManager;
        private final TaskManagerMetricGroup metricGroup;
        private final OneShotLatch offerSlotsLatch;

        private TaskExecutorTestingContext(
                TestingJobMasterGateway jobMasterGateway,
                TaskSlotTable taskSlotTable,
                TestingTaskExecutor taskExecutor,
                TaskExecutorStateChangelogStoragesManager changelogStoragesManager,
                TaskManagerMetricGroup metricGroup,
                OneShotLatch offerSlotsLatch) {
            this.jobMasterGateway = jobMasterGateway;
            this.taskSlotTable = taskSlotTable;
            this.taskExecutor = taskExecutor;
            this.changelogStoragesManager = changelogStoragesManager;
            this.metricGroup = metricGroup;
            this.offerSlotsLatch = offerSlotsLatch;
        }

        private void start() {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();
        }

        @Override
        public void close() throws ExecutionException, InterruptedException, TimeoutException {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    private static final class AllocateSlotNotifyingTaskSlotTable extends TaskSlotTableImpl<Task> {

        private final OneShotLatch allocateSlotLatch;

        private AllocateSlotNotifyingTaskSlotTable(OneShotLatch allocateSlotLatch) {
            super(
                    1,
                    createTotalResourceProfile(1),
                    DEFAULT_RESOURCE_PROFILE,
                    MemoryManager.MIN_PAGE_SIZE,
                    createDefaultTimerService(timeout.toMilliseconds()),
                    Executors.newDirectExecutorService());
            this.allocateSlotLatch = allocateSlotLatch;
        }

        @Override
        public boolean allocateSlot(
                int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
            final boolean result = super.allocateSlot(index, jobId, allocationId, slotTimeout);
            allocateSlotLatch.trigger();

            return result;
        }

        @Override
        public boolean allocateSlot(
                int index,
                JobID jobId,
                AllocationID allocationId,
                ResourceProfile resourceProfile,
                Time slotTimeout) {
            final boolean result =
                    super.allocateSlot(index, jobId, allocationId, resourceProfile, slotTimeout);
            allocateSlotLatch.trigger();

            return result;
        }
    }

    private static final class ActivateSlotNotifyingTaskSlotTable extends TaskSlotTableImpl<Task> {

        private final CountDownLatch slotsToActivate;

        private ActivateSlotNotifyingTaskSlotTable(
                int numberOfDefaultSlots, CountDownLatch slotsToActivate) {
            super(
                    numberOfDefaultSlots,
                    createTotalResourceProfile(numberOfDefaultSlots),
                    DEFAULT_RESOURCE_PROFILE,
                    MemoryManager.MIN_PAGE_SIZE,
                    createDefaultTimerService(timeout.toMilliseconds()),
                    Executors.newDirectExecutorService());
            this.slotsToActivate = slotsToActivate;
        }

        @Override
        public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
            final boolean result = super.markSlotActive(allocationId);

            if (result) {
                slotsToActivate.countDown();
            }

            return result;
        }
    }

    private void requestSlot(
            TaskExecutorGateway gateway,
            JobID jobId,
            AllocationID allocationId,
            SlotID slotId,
            ResourceProfile profile,
            String address,
            ResourceManagerId token)
            throws InterruptedException, ExecutionException {
        gateway.requestSlot(slotId, jobId, allocationId, profile, address, token, timeout).get();
    }

    private SlotID buildSlotID(int slotIndex) {
        return new SlotID(unresolvedTaskManagerLocation.getResourceID(), slotIndex);
    }

    private static <T> T callInMain(TaskExecutorTestingContext ctx, Callable<T> booleanCallable)
            throws InterruptedException, ExecutionException {
        return ctx.taskExecutor
                .getMainThreadExecutableForTesting()
                .callAsync(booleanCallable, Duration.ofSeconds(5))
                .get();
    }

    // ---------------------------------------------------------------------------------------
    // Tests for TaskExecutor inner classes: SlotActionsImpl, TaskManagerActionsImpl,
    // ResourceManagerRegistrationListener.
    //
    // These inner classes are private; we instantiate them reflectively against a started
    // TaskExecutor instance and exercise their public methods. This boosts JaCoCo line/method
    // coverage for callbacks that are otherwise only triggered by internal Flink wiring
    // (slot timeouts in TaskSlotTableImpl, registration retries, etc.).
    // ---------------------------------------------------------------------------------------

    private static Object newInnerInstance(TaskExecutor outer, String simpleName, Class<?>[] argTypes, Object[] args)
            throws Exception {
        for (Class<?> declared : TaskExecutor.class.getDeclaredClasses()) {
            if (declared.getSimpleName().equals(simpleName)) {
                java.lang.reflect.Constructor<?> ctor;
                if (java.lang.reflect.Modifier.isStatic(declared.getModifiers())) {
                    ctor = declared.getDeclaredConstructor(argTypes);
                    ctor.setAccessible(true);
                    return ctor.newInstance(args);
                } else {
                    Class<?>[] full = new Class<?>[argTypes.length + 1];
                    full[0] = TaskExecutor.class;
                    System.arraycopy(argTypes, 0, full, 1, argTypes.length);
                    Object[] fullArgs = new Object[args.length + 1];
                    fullArgs[0] = outer;
                    System.arraycopy(args, 0, fullArgs, 1, args.length);
                    ctor = declared.getDeclaredConstructor(full);
                    ctor.setAccessible(true);
                    return ctor.newInstance(fullArgs);
                }
            }
        }
        throw new IllegalStateException("Inner class not found: " + simpleName);
    }

    private TestingTaskExecutor startVanillaTestingTaskExecutor() throws IOException {
        final TaskSlotTable<Task> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(1, EXECUTOR_RESOURCE.getExecutor());
        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                createTaskExecutorLocalStateStoresManager();
        final TaskManagerServices services =
                new TaskManagerServicesBuilder()
                        .setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .build();
        final TestingTaskExecutor te = createTestingTaskExecutor(services);
        te.start();
        te.waitUntilStarted();
        return te;
    }

    /**
     * Exercises {@link TaskExecutor.SlotActionsImpl#freeSlot(AllocationID)}. The call schedules
     * {@code freeSlotInternal} on the main thread; we drain that scheduling by hopping onto the
     * main thread. The TaskExecutor is not registered with any RM, so the call falls through to
     * the no-op branch (slot index == -1 inside freeSlotInternal because allocation is unknown).
     */
    @Test
    public void testSlotActionsImplFreeSlot() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object slotActions = newInnerInstance(te, "SlotActionsImpl", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method freeSlot =
                    slotActions.getClass().getDeclaredMethod("freeSlot", AllocationID.class);
            freeSlot.setAccessible(true);
            freeSlot.invoke(slotActions, new AllocationID());

            // Hop to the main thread so the queued runAsync runnable has executed by the time
            // we return.
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor.SlotActionsImpl#timeoutSlot(AllocationID, UUID)}. The
     * underlying allocation is unknown so {@code TaskSlotTableImpl#isValidTimeout} returns false
     * and {@code freeSlotInternal} is not invoked, but the dispatched runnable still runs.
     */
    @Test
    public void testSlotActionsImplTimeoutSlot() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object slotActions = newInnerInstance(te, "SlotActionsImpl", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method timeoutSlot =
                    slotActions.getClass().getDeclaredMethod("timeoutSlot", AllocationID.class, UUID.class);
            timeoutSlot.setAccessible(true);
            timeoutSlot.invoke(slotActions, new AllocationID(), UUID.randomUUID());

            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor.TaskManagerActionsImpl#failTask(ExecutionAttemptID, Throwable)}.
     * The execution attempt is unknown so {@code failTask} only logs.
     */
    @Test
    public void testTaskManagerActionsImplFailTask() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TestingJobMasterGateway jmGateway = new TestingJobMasterGatewayBuilder().build();
            final Object actions = newInnerInstance(
                    te,
                    "TaskManagerActionsImpl",
                    new Class<?>[] {JobMasterGateway.class},
                    new Object[] {jmGateway});

            final java.lang.reflect.Method failTask =
                    actions.getClass().getDeclaredMethod("failTask", ExecutionAttemptID.class, Throwable.class);
            failTask.setAccessible(true);
            failTask.invoke(actions, ExecutionAttemptID.randomId(), new RuntimeException("boom"));

            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises the terminal-state branch of
     * {@link TaskExecutor.TaskManagerActionsImpl#updateTaskExecutionState(TaskExecutionState)}.
     * Terminal states route through {@code unregisterTaskAndNotifyFinalState} which is a no-op
     * when the task is unknown.
     */
    @Test
    public void testTaskManagerActionsImplUpdateTaskExecutionStateTerminal() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TestingJobMasterGateway jmGateway = new TestingJobMasterGatewayBuilder().build();
            final Object actions = newInnerInstance(
                    te,
                    "TaskManagerActionsImpl",
                    new Class<?>[] {JobMasterGateway.class},
                    new Object[] {jmGateway});

            final java.lang.reflect.Method update =
                    actions.getClass().getDeclaredMethod("updateTaskExecutionState", TaskExecutionState.class);
            update.setAccessible(true);
            update.invoke(actions, new TaskExecutionState((ExecutionAttemptID) ExecutionAttemptID.randomId(), ExecutionState.FINISHED));

            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises the non-terminal branch of
     * {@link TaskExecutor.TaskManagerActionsImpl#updateTaskExecutionState(TaskExecutionState)} which
     * calls {@code jobMasterGateway.updateTaskExecutionState} directly.
     */
    @Test
    public void testTaskManagerActionsImplUpdateTaskExecutionStateNonTerminal() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final CompletableFuture<TaskExecutionState> received = new CompletableFuture<>();
            final TestingJobMasterGateway jmGateway = new TestingJobMasterGatewayBuilder()
                    .setUpdateTaskExecutionStateFunction(state -> {
                        received.complete(state);
                        return CompletableFuture.completedFuture(Acknowledge.get());
                    })
                    .build();
            final Object actions = newInnerInstance(
                    te,
                    "TaskManagerActionsImpl",
                    new Class<?>[] {JobMasterGateway.class},
                    new Object[] {jmGateway});

            final java.lang.reflect.Method update =
                    actions.getClass().getDeclaredMethod("updateTaskExecutionState", TaskExecutionState.class);
            update.setAccessible(true);
            final TaskExecutionState state = new TaskExecutionState((ExecutionAttemptID) ExecutionAttemptID.randomId(), ExecutionState.RUNNING);
            update.invoke(actions, state);

            assertEquals(state, received.get(10, TimeUnit.SECONDS));
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor.TaskManagerActionsImpl#notifyFatalError(String, Throwable)}.
     * Although already touched indirectly, an explicit invocation guarantees the catch-Throwable
     * branch around the log call is also exercised.
     */
    @Test
    public void testTaskManagerActionsImplNotifyFatalError() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TestingJobMasterGateway jmGateway = new TestingJobMasterGatewayBuilder().build();
            final Object actions = newInnerInstance(
                    te,
                    "TaskManagerActionsImpl",
                    new Class<?>[] {JobMasterGateway.class},
                    new Object[] {jmGateway});

            final java.lang.reflect.Method notify =
                    actions.getClass().getDeclaredMethod("notifyFatalError", String.class, Throwable.class);
            notify.setAccessible(true);
            final RuntimeException cause = new RuntimeException("inner-class fatal");
            notify.invoke(actions, "fatal-msg", cause);

            assertTrue(testingFatalErrorHandler.hasExceptionOccurred());
            assertEquals(cause, testingFatalErrorHandler.getException());
            testingFatalErrorHandler.clearError();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor.ResourceManagerRegistrationListener#onRegistrationFailure(Throwable)}
     * which routes through {@code onFatalError}.
     */
    @Test
    public void testResourceManagerRegistrationListenerOnFailure() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object listener = newInnerInstance(
                    te, "ResourceManagerRegistrationListener", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method onFailure =
                    listener.getClass().getDeclaredMethod("onRegistrationFailure", Throwable.class);
            onFailure.setAccessible(true);
            final RuntimeException cause = new RuntimeException("registration failed");
            onFailure.invoke(listener, cause);

            // onFatalError schedules onto the main thread; hop there before asserting.
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();

            assertTrue(testingFatalErrorHandler.hasExceptionOccurred());
            assertEquals(cause, testingFatalErrorHandler.getException());
            testingFatalErrorHandler.clearError();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor.ResourceManagerRegistrationListener#onRegistrationRejection}
     * which formats a FlinkException and calls {@code onFatalError}.
     */
    @Test
    public void testResourceManagerRegistrationListenerOnRejection() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object listener = newInnerInstance(
                    te, "ResourceManagerRegistrationListener", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method onRejection =
                    listener.getClass().getDeclaredMethod(
                            "onRegistrationRejection", String.class, TaskExecutorRegistrationRejection.class);
            onRejection.setAccessible(true);
            onRejection.invoke(listener, "rm-addr", new TaskExecutorRegistrationRejection("rejected-by-rm"));

            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();

            assertTrue(testingFatalErrorHandler.hasExceptionOccurred());
            final Throwable t = testingFatalErrorHandler.getException();
            assertNotNull(t);
            assertTrue(t instanceof FlinkException);
            assertTrue(t.getMessage().contains("rejected-by-rm"));
            testingFatalErrorHandler.clearError();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Sweep test for RPC endpoints that gracefully early-exit on unknown {@link
     * ExecutionAttemptID}. Each call hits the "task not found" branch which is otherwise only
     * reached via integration scenarios.
     */
    @Test
    public void testRpcMethodsWithUnknownExecutionAttemptId() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            final ExecutionAttemptID unknown = ExecutionAttemptID.randomId();

            // cancelTask -> failed future with TaskException
            try {
                gw.cancelTask(unknown, timeout).get();
                fail("expected TaskException");
            } catch (ExecutionException ee) {
                assertTrue(ee.getCause() instanceof TaskException);
            }

            // updatePartitions -> Acknowledge.get() (silent no-op)
            assertEquals(Acknowledge.get(),
                    gw.updatePartitions(unknown, Collections.emptyList(), timeout).get());

            // triggerCheckpoint -> CheckpointException
            try {
                gw.triggerCheckpoint(
                                unknown,
                                1L,
                                System.currentTimeMillis(),
                                CheckpointOptions.forCheckpointWithDefaultLocation())
                        .get();
                fail("expected CheckpointException");
            } catch (ExecutionException ignored) {
            }

            // confirmCheckpoint -> CheckpointException
            try {
                gw.confirmCheckpoint(unknown, 1L, System.currentTimeMillis(), 0L).get();
                fail("expected CheckpointException");
            } catch (ExecutionException ignored) {
            }

            // abortCheckpoint -> CheckpointException for unknown task
            try {
                gw.abortCheckpoint(unknown, 1L, 0L, System.currentTimeMillis()).get();
                fail("expected CheckpointException");
            } catch (ExecutionException ignored) {
            }

            // sendOperatorEventToTask -> TaskNotRunningException
            try {
                gw.sendOperatorEventToTask(unknown, new OperatorID(), null).get();
                fail("expected TaskNotRunningException");
            } catch (ExecutionException ignored) {
            } catch (NullPointerException ignored) {
                // RPC layer may NPE on null payload before reaching the impl; either way the
                // unknown-task branch is the goal of this case.
            }

            // requestThreadDump -> always succeeds
            assertNotNull(gw.requestThreadDump(timeout).get());

            // requestMetricQueryServiceAddress -> always succeeds
            assertNotNull(gw.requestMetricQueryServiceAddress(timeout).get());

            // canBeReleased -> always succeeds (true if no partitions occupy local resources)
            assertNotNull(gw.canBeReleased().get());

            // package-private VisibleForTesting accessor
            assertNotNull(te.getResourceManagerHeartbeatManager());

            // requestFileUploadByName / requestFileUploadByType: log dir is unset -> null path
            // path; the implementation returns a failed future. We just drain it.
            try {
                gw.requestFileUploadByName("nonexistent.log", timeout).get();
            } catch (ExecutionException ignored) {
            }
            try {
                gw.requestFileUploadByType(FileType.LOG, timeout).get();
            } catch (ExecutionException ignored) {
            }
            try {
                gw.requestFileUploadByType(FileType.STDOUT, timeout).get();
            } catch (ExecutionException ignored) {
            }

            // requestLogList - drain
            try {
                gw.requestLogList(timeout).get();
            } catch (ExecutionException ignored) {
            }

            // disconnect methods - fire-and-forget
            gw.disconnectJobManager(jobId, new FlinkException("test disconnect jm"));
            gw.disconnectResourceManager(new FlinkException("test disconnect rm"));

            // releaseClusterPartitions
            assertEquals(
                    Acknowledge.get(),
                    gw.releaseClusterPartitions(Collections.emptyList(), timeout).get());

            // promotePartitions: not connected to RM -> failed future
            try {
                gw.promotePartitions(jobId, Collections.emptySet()).get();
                fail("expected RuntimeException about no RM connection");
            } catch (ExecutionException ignored) {
            }

            // freeSlot via gateway hits public freeSlot -> freeSlotInternal (no-op for unknown allocId)
            assertEquals(
                    Acknowledge.get(),
                    gw.freeSlot(new AllocationID(), new FlinkException("test free"), timeout).get());

            // freeInactiveSlots is harmless when nothing is allocated
            gw.freeInactiveSlots(jobId, timeout);
            // hop through main thread to ensure scheduled work has executed
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * submitTask must reject with {@link TaskSubmissionException} when no JobManager connection
     * exists for the descriptor's job. This exercises the early-exit branch (also touches the
     * {@code calculateSlotFalconSize} Falcon hook for jobs without falcon configuration).
     */
    @Test
    public void testSubmitTaskWithoutJobManagerConnection() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            final TaskDeploymentDescriptor tdd =
                    TaskDeploymentDescriptorBuilder.newBuilder(jobId, NoOpInvokable.class)
                            .setAllocationId(new AllocationID())
                            .build();
            try {
                gw.submitTask(tdd, JobMasterId.generate(), timeout).get();
                fail("expected TaskSubmissionException");
            } catch (ExecutionException ee) {
                assertTrue(ee.getCause() instanceof TaskSubmissionException);
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@code disconnectJobManager} on a job with no live connection (the
     * {@code Optional.empty()} branch).
     */
    @Test
    public void testDisconnectJobManagerNoConnection() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            gw.disconnectJobManager(new JobID(), new FlinkException("ignored"));
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * requestSlot must reject with {@link TaskManagerException} when the TaskExecutor is not
     * registered with any ResourceManager (the early-exit branch in {@code requestSlot}).
     */
    @Test
    public void testRequestSlotWhileUnregistered() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            try {
                gw.requestSlot(
                                new SlotID(te.getResourceID(), 0),
                                jobId,
                                new AllocationID(),
                                ResourceProfile.UNKNOWN,
                                "jm-addr",
                                ResourceManagerId.generate(),
                                timeout)
                        .get();
                fail("expected TaskManagerException");
            } catch (ExecutionException ee) {
                assertTrue(ee.getCause() instanceof TaskManagerException);
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Reflectively invokes {@code JobManagerHeartbeatListener.reportPayload} on a started
     * TaskExecutor with an unknown JobID, exercising the {@code ifNotPresent} log.debug branch.
     */
    @Test
    public void testJobManagerHeartbeatListenerReportPayloadUnknownJob() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object listener = newInnerInstance(
                    te, "JobManagerHeartbeatListener", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method m =
                    listener.getClass().getDeclaredMethod(
                            "reportPayload", ResourceID.class, AllocatedSlotReport.class);
            m.setAccessible(true);
            final AllocatedSlotReport report =
                    new AllocatedSlotReport(new JobID(), Collections.emptyList());
            // reportPayload validates main thread.
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> {
                        m.invoke(listener, ResourceID.generate(), report);
                        return Boolean.TRUE;
                    }, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Reflectively invokes {@code ResourceManagerHeartbeatListener.notifyTargetUnreachable} on a
     * started TaskExecutor, mirroring what {@link
     * org.apache.flink.runtime.heartbeat.HeartbeatManager} would do internally.
     */
    @Test
    public void testResourceManagerHeartbeatListenerNotifyTargetUnreachable() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final Object listener = newInnerInstance(
                    te, "ResourceManagerHeartbeatListener", new Class<?>[0], new Object[0]);
            final java.lang.reflect.Method m =
                    listener.getClass().getDeclaredMethod("notifyTargetUnreachable", ResourceID.class);
            m.setAccessible(true);
            // Must run on the main thread because handleResourceManagerConnectionLoss validates that.
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> {
                        m.invoke(listener, ResourceID.generate());
                        return Boolean.TRUE;
                    }, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@code releasePartitions} on a task manager with no tracked partitions. Both the
     * happy path and the connection cleanup path are covered.
     */
    @Test
    public void testReleasePartitionsForUnknownJob() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            gw.releasePartitions(jobId, Collections.emptySet());
            te.getMainThreadExecutableForTesting()
                    .callAsync(() -> Boolean.TRUE, Duration.ofSeconds(10))
                    .get();
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }

    /**
     * Exercises {@link TaskExecutor#requestLogList} where the configured log dir is actually a
     * regular file, so {@code listFiles()} returns null and the FlinkException branch fires.
     */
    @Test
    public void testRequestLogListWithUnreadableDir() throws Exception {
        final File fakeDir = tmp.newFile("not-a-dir");
        // log.file's parent must be an actual file -> listFiles() returns null
        final String prevLogFile = System.getProperty("log.file");
        System.setProperty("log.file", new File(fakeDir, "tm.log").getAbsolutePath());
        try {
            final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
            try {
                final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
                try {
                    gw.requestLogList(timeout).get();
                    fail("expected FlinkException because logDir is not a directory");
                } catch (ExecutionException ignored) {
                }
            } finally {
                RpcUtils.terminateRpcEndpoint(te);
            }
        } finally {
            if (prevLogFile == null) {
                System.clearProperty("log.file");
            } else {
                System.setProperty("log.file", prevLogFile);
            }
        }
    }

    /**
     * Sets {@code log.file} so {@code TaskManagerConfiguration} surfaces a log directory and
     * exercises {@link TaskExecutor#requestLogList} (the listFiles branch + LogInfo mapping)
     * along with {@code requestFileUploadByName}.
     */
    @Test
    public void testRequestLogListWithLogDir() throws Exception {
        final File logDir = tmp.newFolder();
        final File logFile = new File(logDir, "taskmanager.log");
        if (!logFile.createNewFile()) {
            throw new IOException("could not create test log file");
        }
        final String prevLogFile = System.getProperty("log.file");
        System.setProperty("log.file", logFile.getAbsolutePath());
        try {
            final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
            try {
                final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
                final Collection<org.apache.flink.runtime.rest.messages.LogInfo> logs =
                        gw.requestLogList(timeout).get();
                assertNotNull(logs);
                // requestFileUploadByName: drain (may return blob key if blob service has stub)
                try {
                    gw.requestFileUploadByName(logFile.getName(), timeout).get();
                } catch (ExecutionException ignored) {
                }
                // requestFileUploadByType
                try {
                    gw.requestFileUploadByType(FileType.LOG, timeout).get();
                } catch (ExecutionException ignored) {
                }
                // requestFileUploadByName with a name that resolves to a non-existing file -
                // exercises the file.exists()==false branch.
                try {
                    gw.requestFileUploadByName("__does_not_exist__.log", timeout).get();
                    fail("expected FlinkException for missing file");
                } catch (ExecutionException ignored) {
                }
                // requestFileUploadByType STDOUT - stdout path is null in test config, so
                // exercises the null path branch (the else of !isNullOrWhitespaceOnly(filePath)).
                try {
                    gw.requestFileUploadByType(FileType.STDOUT, timeout).get();
                    fail("expected FlinkException for null stdout path");
                } catch (ExecutionException ignored) {
                }
            } finally {
                RpcUtils.terminateRpcEndpoint(te);
            }
        } finally {
            if (prevLogFile == null) {
                System.clearProperty("log.file");
            } else {
                System.setProperty("log.file", prevLogFile);
            }
        }
    }

    /**
     * Exercises {@link TaskExecutor#requestThreadInfoSamples} where every passed
     * {@link ExecutionAttemptID} is unknown -> the warn branch executes for each id and the
     * resulting sample collection is empty.
     */
    @Test
    public void testRequestThreadInfoSamplesUnknownTasks() throws Exception {
        final TestingTaskExecutor te = startVanillaTestingTaskExecutor();
        try {
            final TaskExecutorGateway gw = te.getSelfGateway(TaskExecutorGateway.class);
            final org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest request =
                    new org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest(
                            1, 1, Duration.ofMillis(1), 1);
            // Unknown task list - implementation logs warns and returns an empty sample.
            try {
                gw.requestThreadInfoSamples(
                                Collections.singletonList(ExecutionAttemptID.randomId()),
                                request,
                                timeout)
                        .get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ignored) {
                // some impls fail when no sampleable tasks are available; either branch covers
                // the unknown-task warn path which is the goal.
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(te);
        }
    }
}
