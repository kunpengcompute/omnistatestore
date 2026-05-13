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

package org.apache.flink.api.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.mockito.Mock;
import org.rocksdb.RocksDB;

public class ExecutionConfigTest extends TestLogger {

    @Test
    public void testDoubleTypeRegistration() {
        ExecutionConfig config = new ExecutionConfig();
        List<Class<?>> types = Arrays.<Class<?>>asList(Double.class, Integer.class, Double.class);
        List<Class<?>> expectedTypes = Arrays.<Class<?>>asList(Double.class, Integer.class);

        for (Class<?> tpe : types) {
            config.registerKryoType(tpe);
        }

        int counter = 0;

        for (Class<?> tpe : config.getRegisteredKryoTypes()) {
            assertEquals(tpe, expectedTypes.get(counter++));
        }

        assertEquals(expectedTypes.size(), counter);
    }

    @Test
    public void testConfigurationOfParallelism() {
        ExecutionConfig config = new ExecutionConfig();

        // verify explicit change in parallelism
        int parallelism = 36;
        config.setParallelism(parallelism);

        assertEquals(parallelism, config.getParallelism());

        // verify that parallelism is reset to default flag value
        parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        config.setParallelism(parallelism);

        assertEquals(parallelism, config.getParallelism());
    }

    @Test
    public void testDisableGenericTypes() {
        ExecutionConfig conf = new ExecutionConfig();
        TypeInformation<Object> typeInfo = new GenericTypeInfo<Object>(Object.class);

        // by default, generic types are supported
        TypeSerializer<Object> serializer = typeInfo.createSerializer(conf);
        assertTrue(serializer instanceof KryoSerializer);

        // expect an exception when generic types are disabled
        conf.disableGenericTypes();
        try {
            typeInfo.createSerializer(conf);
            fail("should have failed with an exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
        final Random r = new Random();

        final int parallelism = 1 + r.nextInt(10);
        final boolean closureCleanerEnabled = r.nextBoolean(),
            forceAvroEnabled = r.nextBoolean(),
            forceKryoEnabled = r.nextBoolean(),
            disableGenericTypes = r.nextBoolean(),
            objectReuseEnabled = r.nextBoolean();

        final ExecutionConfig config = new ExecutionConfig();

        if (closureCleanerEnabled) {
            config.enableClosureCleaner();
        } else {
            config.disableClosureCleaner();
        }
        if (forceAvroEnabled) {
            config.enableForceAvro();
        } else {
            config.disableForceAvro();
        }
        if (forceKryoEnabled) {
            config.enableForceKryo();
        } else {
            config.disableForceKryo();
        }
        if (disableGenericTypes) {
            config.disableGenericTypes();
        } else {
            config.enableGenericTypes();
        }
        if (objectReuseEnabled) {
            config.enableObjectReuse();
        } else {
            config.disableObjectReuse();
        }
        config.setParallelism(parallelism);

        final ExecutionConfig copy1 = CommonTestUtils.createCopySerializable(config);
        final ExecutionConfig copy2 = new SerializedValue<>(config).deserializeValue(getClass().getClassLoader());

        assertNotNull(copy1);
        assertNotNull(copy2);

        assertEquals(config, copy1);
        assertEquals(config, copy2);

        assertEquals(closureCleanerEnabled, copy1.isClosureCleanerEnabled());
        assertEquals(forceAvroEnabled, copy1.isForceAvroEnabled());
        assertEquals(forceKryoEnabled, copy1.isForceKryoEnabled());
        assertEquals(disableGenericTypes, copy1.hasGenericTypesDisabled());
        assertEquals(objectReuseEnabled, copy1.isObjectReuseEnabled());
        assertEquals(parallelism, copy1.getParallelism());
    }

    @Test
    public void testGlobalParametersNotNull() {
        final ExecutionConfig config = new ExecutionConfig();

        assertNotNull(config.getGlobalJobParameters());
    }

    @Test
    public void testGlobalParametersHashCode() {
        ExecutionConfig config = new ExecutionConfig();
        ExecutionConfig anotherConfig = new ExecutionConfig();

        assertEquals(config.getGlobalJobParameters().hashCode(), anotherConfig.getGlobalJobParameters().hashCode());
    }

    @Test
    public void testReadingDefaultConfig() {
        ExecutionConfig executionConfig = new ExecutionConfig();
        Configuration configuration = new Configuration();

        // mutate config according to configuration
        executionConfig.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        assertThat(executionConfig, equalTo(new ExecutionConfig()));
    }

    @Test
    public void testLoadingRegisteredKryoTypesFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.registerKryoType(ExecutionConfigTest.class);
        configFromSetters.registerKryoType(TestSerializer1.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
            "pipeline.registered-kryo-types",
            "org.apache.flink.api.common.ExecutionConfigTest;" +
                "org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1"
        );

        // mutate config according to configuration
        configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration, equalTo(configFromSetters));
    }

    @Test
    public void testLoadingRegisteredPojoTypesFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.registerPojoType(ExecutionConfigTest.class);
        configFromSetters.registerPojoType(TestSerializer1.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
            "pipeline.registered-pojo-types",
            "org.apache.flink.api.common.ExecutionConfigTest;" +
                "org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1"
        );

        // mutate config according to configuration
        configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration, equalTo(configFromSetters));
    }

    @Test
    public void testLoadingRestartStrategyFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.minutes(2)));

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString("restart-strategy", "fixeddelay");
        configuration.setString("restart-strategy.fixed-delay.attempts", "10");
        configuration.setString("restart-strategy.fixed-delay.delay", "2 min");

        // mutate config according to configuration
        configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration, equalTo(configFromSetters));
    }

    @Test
    public void testLoadingDefaultKryoSerializersFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.addDefaultKryoSerializer(ExecutionConfigTest.class, TestSerializer1.class);
        configFromSetters.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
            "pipeline.default-kryo-serializers",
            "class:org.apache.flink.api.common.ExecutionConfigTest," +
                "serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1;" +
                "class:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1," +
                "serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer2"
        );

        // mutate config according to configuration
        configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration, equalTo(configFromSetters));
    }

    @Test
    public void testLoadingIsDynamicGraphFromConfiguration() {
        testLoadingIsDynamicGraphFromConfiguration(JobManagerOptions.SchedulerType.AdaptiveBatch, true);
        testLoadingIsDynamicGraphFromConfiguration(JobManagerOptions.SchedulerType.Default, false);
        testLoadingIsDynamicGraphFromConfiguration(JobManagerOptions.SchedulerType.Adaptive, false);
    }

    private void testLoadingIsDynamicGraphFromConfiguration(
        JobManagerOptions.SchedulerType schedulerType,
        boolean expectIsDynamicGraph
    ) {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, schedulerType);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();
        configFromConfiguration.configure(configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration.isDynamicGraph(), is(expectIsDynamicGraph));
    }

    @Test
    public void testNotOverridingRegisteredKryoTypesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.registerKryoType(ExecutionConfigTest.class);
        config.registerKryoType(TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(ExecutionConfigTest.class);
        set.add(TestSerializer1.class);
        assertThat(config.getRegisteredKryoTypes(), equalTo(set));
    }

    @Test
    public void testNotOverridingRegisteredPojoTypesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(ExecutionConfigTest.class);
        config.registerPojoType(TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(ExecutionConfigTest.class);
        set.add(TestSerializer1.class);
        assertThat(config.getRegisteredPojoTypes(), equalTo(set));
    }

    @Test
    public void testNotOverridingRestartStrategiesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
            RestartStrategies.fixedDelayRestart(10, Time.minutes(2));
        config.setRestartStrategy(restartStrategyConfiguration);

        // mutate config according to configuration
        config.configure(new Configuration(), Thread.currentThread().getContextClassLoader());

        assertThat(config.getRestartStrategy(), equalTo(restartStrategyConfiguration));
    }

    @Test
    public void testNotOverridingDefaultKryoSerializersFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.addDefaultKryoSerializer(ExecutionConfigTest.class, TestSerializer1.class);
        config.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashMap<Class<?>, Class<? extends Serializer>> serialiers = new LinkedHashMap<>();
        serialiers.put(ExecutionConfigTest.class, TestSerializer1.class);
        serialiers.put(TestSerializer1.class, TestSerializer2.class);
        assertThat(config.getDefaultKryoSerializerClasses(), equalTo(serialiers));
    }

    private static class TestSerializer1 extends Serializer<ExecutionConfigTest> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, ExecutionConfigTest object) {}

        @Override
        public ExecutionConfigTest read(Kryo kryo, Input input, Class<ExecutionConfigTest> type) {
            return null;
        }
    }

    private static class TestSerializer2 extends Serializer<TestSerializer1> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, TestSerializer1 object) {}

        @Override
        public TestSerializer1 read(Kryo kryo, Input input, Class<TestSerializer1> type) {
            return null;
        }
    }

    @Test
    public void testSetSubTaskFalconSize() {
        ExecutionConfig config = new ExecutionConfig();
        config.setSubTaskFalconSize(0);
        assertEquals(0, config.getSubTaskFalconSize());

        config.setSubTaskFalconSize(42);
        assertEquals(42, config.getSubTaskFalconSize());
    }

    @Test
    public void testGetSubTaskFalconSize() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(0, config.getSubTaskFalconSize());
    }

    @Test
    public void testEnableClosureCleaner() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableClosureCleaner();
        assertFalse(config.isClosureCleanerEnabled());
        config.enableClosureCleaner();
        assertTrue(config.isClosureCleanerEnabled());
    }

    @Test
    public void testDisableClosureCleaner() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableClosureCleaner();
        assertFalse(config.isClosureCleanerEnabled());
    }

    @Test
    public void testIsClosureCleanerEnabled() {
        ExecutionConfig config = new ExecutionConfig();
        assertTrue(config.isClosureCleanerEnabled());
    }

    @Test
    public void testSetClosureCleanerLevel() {
        ExecutionConfig config = new ExecutionConfig();
        config.setClosureCleanerLevel(ExecutionConfig.ClosureCleanerLevel.NONE);
        assertEquals(ExecutionConfig.ClosureCleanerLevel.NONE, config.getClosureCleanerLevel());

        config.setClosureCleanerLevel(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL);
        assertEquals(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL, config.getClosureCleanerLevel());
    }

    @Test
    public void testGetClosureCleanerLevel() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(ExecutionConfig.ClosureCleanerLevel.RECURSIVE, config.getClosureCleanerLevel());
    }

    @Test
    public void testSetAutoWatermarkInterval() {
        ExecutionConfig config = new ExecutionConfig();
        config.setAutoWatermarkInterval(0L);
        assertEquals(0L, config.getAutoWatermarkInterval());

        config.setAutoWatermarkInterval(500L);
        assertEquals(500L, config.getAutoWatermarkInterval());
    }

    @Test
    public void testGetAutoWatermarkInterval() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(200L, config.getAutoWatermarkInterval());
    }

    @Test
    public void testSetLatencyTrackingInterval() {
        ExecutionConfig config = new ExecutionConfig();
        config.setLatencyTrackingInterval(0L);
        assertEquals(0L, config.getLatencyTrackingInterval());
    }

    @Test
    public void testGetLatencyTrackingInterval() {
        ExecutionConfig config = new ExecutionConfig();
        long interval = config.getLatencyTrackingInterval();
        assertEquals(interval, config.getLatencyTrackingInterval());
        assertTrue(interval >= 0);
    }

    @Test
    public void testIsLatencyTrackingConfigured() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isLatencyTrackingConfigured());
        config.setLatencyTrackingInterval(100L);
        assertTrue(config.isLatencyTrackingConfigured());
    }

    @Test
    public void testGetPeriodicMaterializeIntervalMillis() {
        ExecutionConfig config = new ExecutionConfig();
        long interval = config.getPeriodicMaterializeIntervalMillis();
        assertEquals(interval, config.getPeriodicMaterializeIntervalMillis());
        assertTrue(interval >= 0);
    }

    @Test
    public void testSetPeriodicMaterializeIntervalMillis() {
        Duration duration = Duration.ZERO;
        ExecutionConfig config = new ExecutionConfig();
        config.setPeriodicMaterializeIntervalMillis(duration);
        assertEquals(0L, config.getPeriodicMaterializeIntervalMillis());
    }

    @Test
    public void testGetMaterializationMaxAllowedFailures() {
        ExecutionConfig config = new ExecutionConfig();
        int failures = config.getMaterializationMaxAllowedFailures();
        assertEquals(failures, config.getMaterializationMaxAllowedFailures());
        assertTrue(failures >= 0);
    }

    @Test
    public void testSetMaterializationMaxAllowedFailures() {
        ExecutionConfig config = new ExecutionConfig();
        config.setMaterializationMaxAllowedFailures(0);
        assertEquals(0, config.getMaterializationMaxAllowedFailures());

        config.setMaterializationMaxAllowedFailures(5);
        assertEquals(5, config.getMaterializationMaxAllowedFailures());
    }

    @Test
    public void testGetMaxParallelism() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(-1, config.getMaxParallelism());
    }

    @Test
    public void testSetMaxParallelism() {
        ExecutionConfig config = new ExecutionConfig();
        config.setMaxParallelism(1);
        assertEquals(1, config.getMaxParallelism());
    }

    @Test
    public void testSetTaskCancellationInterval() {
        ExecutionConfig config = new ExecutionConfig();
        config.setTaskCancellationInterval(0);
        assertEquals(0, config.getTaskCancellationInterval());

        config.setTaskCancellationInterval(5000L);
        assertEquals(5000L, config.getTaskCancellationInterval());
    }

    @Test
    public void testSetTaskCancellationTimeout() {
        ExecutionConfig config = new ExecutionConfig();
        config.setTaskCancellationTimeout(0);
        assertEquals(0, config.getTaskCancellationTimeout());

        config.setTaskCancellationTimeout(30000L);
        assertEquals(30000L, config.getTaskCancellationTimeout());
    }

    @Test
    public void testGetNumberOfExecutionRetries() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(-1, config.getNumberOfExecutionRetries());
    }

    @Test
    public void testGetExecutionRetryDelay() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(10000L, config.getExecutionRetryDelay());
    }

    @Test
    public void testSetNumberOfExecutionRetries() {
        ExecutionConfig config = new ExecutionConfig();
        config.setNumberOfExecutionRetries(0);
        assertEquals(0, config.getNumberOfExecutionRetries());

        config.setNumberOfExecutionRetries(3);
        assertEquals(3, config.getNumberOfExecutionRetries());
    }

    @Test
    public void testSetExecutionRetryDelay() {
        ExecutionConfig config = new ExecutionConfig();
        config.setExecutionRetryDelay(0);
        assertEquals(0L, config.getExecutionRetryDelay());

        config.setExecutionRetryDelay(5000L);
        assertEquals(5000L, config.getExecutionRetryDelay());
    }

    @Test
    public void testSetExecutionMode() {
        ExecutionConfig config = new ExecutionConfig();
        config.setExecutionMode(ExecutionMode.BATCH);
        assertEquals(ExecutionMode.BATCH, config.getExecutionMode());
    }

    @Test
    public void testGetExecutionMode() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(ExecutionMode.PIPELINED, config.getExecutionMode());
    }

    @Test
    public void testSetDefaultInputDependencyConstraint() {
        ExecutionConfig config = new ExecutionConfig();
        config.setDefaultInputDependencyConstraint(InputDependencyConstraint.ANY);
        assertEquals(InputDependencyConstraint.ANY, config.getDefaultInputDependencyConstraint());
    }

    @Test
    public void testGetDefaultInputDependencyConstraint() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(InputDependencyConstraint.ANY, config.getDefaultInputDependencyConstraint());
    }

    @Test
    public void testEnableForceKryo() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isForceKryoEnabled());
        config.enableForceKryo();
        assertTrue(config.isForceKryoEnabled());
    }

    @Test
    public void testEnableGenericTypes() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();
        assertTrue(config.hasGenericTypesDisabled());
        config.enableGenericTypes();
        assertFalse(config.hasGenericTypesDisabled());
    }

    @Test
    public void testEnableAutoGeneratedUIDs() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableAutoGeneratedUIDs();
        assertFalse(config.hasAutoGeneratedUIDsEnabled());
        config.enableAutoGeneratedUIDs();
        assertTrue(config.hasAutoGeneratedUIDsEnabled());
    }

    @Test
    public void testDisableAutoGeneratedUIDs() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableAutoGeneratedUIDs();
        assertFalse(config.hasAutoGeneratedUIDsEnabled());
    }

    @Test
    public void testHasAutoGeneratedUIDsEnabled() {
        ExecutionConfig config = new ExecutionConfig();
        assertTrue(config.hasAutoGeneratedUIDsEnabled());
    }

    @Test
    public void testEnableForceAvro() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isForceAvroEnabled());
        config.enableForceAvro();
        assertTrue(config.isForceAvroEnabled());
    }

    @Test
    public void testEnableObjectReuse() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isObjectReuseEnabled());
        config.enableObjectReuse();
        assertTrue(config.isObjectReuseEnabled());
    }

    @Test
    public void testSetGlobalJobParameters() {
        ExecutionConfig config = new ExecutionConfig();
        ExecutionConfig.GlobalJobParameters params = new ExecutionConfig.GlobalJobParameters();
        config.setGlobalJobParameters(params);
        assertEquals(params, config.getGlobalJobParameters());
    }

    @Test
    public void testAddDefaultKryoSerializer() {
        ExecutionConfig config = new ExecutionConfig();
        TestSerializer2 serializerInstance = new TestSerializer2();
        config.addDefaultKryoSerializer(TestSerializer1.class, serializerInstance);
        assertTrue(config.getDefaultKryoSerializers().containsKey(TestSerializer1.class));

        config.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);
        assertTrue(config.getDefaultKryoSerializerClasses().containsKey(TestSerializer1.class));
        assertEquals(TestSerializer2.class, config.getDefaultKryoSerializerClasses().get(TestSerializer1.class));
    }

    @Test
    public void testRegisterTypeWithKryoSerializer() {
        ExecutionConfig config = new ExecutionConfig();
        TestSerializer2 serializerInstance = new TestSerializer2();
        config.registerTypeWithKryoSerializer(TestSerializer1.class, serializerInstance);
        assertTrue(config.getRegisteredTypesWithKryoSerializers().containsKey(TestSerializer1.class));

        config.registerTypeWithKryoSerializer(TestSerializer1.class, TestSerializer2.class);
        assertTrue(config.getRegisteredTypesWithKryoSerializerClasses().containsKey(TestSerializer1.class));
        assertEquals(
            TestSerializer2.class,
            config.getRegisteredTypesWithKryoSerializerClasses().get(TestSerializer1.class)
        );
    }

    @Test
    public void testIsAutoTypeRegistrationDisabled() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isAutoTypeRegistrationDisabled());
    }

    @Test
    public void testDisableAutoTypeRegistration() {
        ExecutionConfig config = new ExecutionConfig();
        config.disableAutoTypeRegistration();
        assertTrue(config.isAutoTypeRegistrationDisabled());
    }

    @Test
    public void testSetUseSnapshotCompression() {
        ExecutionConfig config = new ExecutionConfig();
        assertFalse(config.isUseSnapshotCompression());
        config.setUseSnapshotCompression(true);
        assertTrue(config.isUseSnapshotCompression());
        config.setUseSnapshotCompression(false);
        assertFalse(config.isUseSnapshotCompression());
    }

    @Test
    public void testHashCode() {
        ExecutionConfig config = new ExecutionConfig();
        assertEquals(config.hashCode(), config.hashCode());
    }

    @Test
    public void testToString() {
        ExecutionConfig config = new ExecutionConfig();
        String result = config.toString();
        assertNotNull(result);
        assertTrue(result.length() > 0);
    }

    @Test
    public void testArchive() {
        ExecutionConfig config = new ExecutionConfig();
        assertNotNull(config.archive());
    }

    @Test
    public void testGetSerializer() {
        MySerializer mySerializer = new MySerializer();
        ExecutionConfig.SerializableSerializer<MySerializer> wrappedSerializer =
            new ExecutionConfig.SerializableSerializer<>(mySerializer);
        assertNotNull(wrappedSerializer.getSerializer());
        assertEquals(mySerializer, wrappedSerializer.getSerializer());
    }

    private static class MySerializer extends Serializer<String> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, String object) {}

        @Override
        public String read(Kryo kryo, Input input, Class<String> type) {
            return "";
        }
    }

    @Test
    public void testGetDescription() {
        assertNotNull(ExecutionConfig.ClosureCleanerLevel.NONE.getDescription());
        assertNotNull(ExecutionConfig.ClosureCleanerLevel.RECURSIVE.getDescription());
        assertNotNull(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL.getDescription());
    }

    @Test
    public void testMapBasedJobParameters() {
        Configuration config = new Configuration();
        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("key1", "value1");
        jobParams.put("key2", "value2");
        config.set(PipelineOptions.GLOBAL_JOB_PARAMETERS, jobParams);

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.configure(config, ClassLoader.getSystemClassLoader());

        ExecutionConfig.GlobalJobParameters globalParams = executionConfig.getGlobalJobParameters();
        assertNotNull(globalParams);

        Map<String, String> resultMap = globalParams.toMap();
        assertNotNull(resultMap);
        assertEquals("value1", resultMap.get("key1"));
        assertEquals("value2", resultMap.get("key2"));

        assertFalse(globalParams.equals(""));
        assertTrue(globalParams.equals(globalParams));

        int hashCode = globalParams.hashCode();
        assertEquals(hashCode, globalParams.hashCode());
    }
}
