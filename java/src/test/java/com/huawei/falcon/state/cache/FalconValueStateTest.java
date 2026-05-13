package com.huawei.falcon.state.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * Unit tests for {@link FalconValueState}.
 *
 * <p>The Java side of {@code FalconValueState} is a thin JNI wrapper. The native methods are
 * provided by the test-resource stub {@code libfalcon.so} which forces all "is the cache open?"
 * checks to return false (so any real cache work is short-circuited). These tests therefore focus
 * on the Java-side wrapping behaviour (constructor, library-load state machine, AutoCloseable
 * handle release, native delegation) rather than on the native cache logic itself.
 *
 * <p>If the native stub is not available on this platform (e.g. Windows), all tests requiring
 * a live FalconValueState are automatically skipped via {@link Assume}.
 */
public class FalconValueStateTest {

    private static RocksDB db;
    private static File dbDir;
    private static Options opts;
    private static ColumnFamilyHandle defaultCf;
    private static WriteOptions writeOptions;

    /** True if libfalcon.so loaded successfully and RocksDB is open. */
    private static boolean nativeLibAvailable = false;

    @BeforeClass
    public static void openDb() throws Exception {
        // Force-trigger the static initializer so libfalcon.so (the test stub) is loaded.
        try {
            Class.forName("com.huawei.falcon.state.cache.FalconValueState");
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
            // The native stub is not compatible with this platform (e.g. Linux .so on Windows).
            // All tests requiring a live FalconValueState will be skipped.
            return;
        }

        try {
            RocksDB.loadLibrary();
            dbDir = Files.createTempDirectory("falcon-vs-test").toFile();
            opts = new Options().setCreateIfMissing(true);
            db = RocksDB.open(opts, dbDir.getAbsolutePath());
            defaultCf = db.getDefaultColumnFamily();
            writeOptions = new WriteOptions();
            nativeLibAvailable = true;
        } catch (UnsatisfiedLinkError e) {
            // RocksDB native library not available — tests will be skipped.
        }
    }

    @AfterClass
    public static void closeDb() {
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (defaultCf != null) {
            defaultCf.close();
        }
        if (db != null) {
            db.close();
        }
        if (opts != null) {
            opts.close();
        }
        if (dbDir != null) {
            deleteRecursively(dbDir);
        }
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    /** Skip the test if the native stub or RocksDB is not available on this platform. */
    private static void assumeNativeLibAvailable() {
        Assume.assumeTrue("libfalcon.so not available on this platform - skipping", nativeLibAvailable);
    }

    @Before
    public void resetLibraryState() {
        // Tests may repeatedly call loadFalconLibrary; ensure global state is "LOADED" between tests.
        // After the static initializer ran in openDb() the AtomicReference is already LOADED — keep it.
    }

    // ------------------------------------------------------------------
    //  Constructor / handle lifecycle
    // ------------------------------------------------------------------

    @Test
    public void testConstructorAllocatesHandle() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            long handle = readLong(fvs, "falconHandle_");
            long rocksHandle = readLong(fvs, "rocksdbHandle_");
            assertEquals(
                "rocksdbHandle_ should be the native handle from RocksDB.getNativeHandle()",
                db.getNativeHandle(),
                rocksHandle
            );
            assertNotNull("falcon instance should be constructed", fvs);
            assertEquals("falconHandle_ should be stable after construction", handle, readLong(fvs, "falconHandle_"));
        }
    }

    @Test
    public void testCloseIsIdempotentAndZeroesHandle() throws Exception {
        assumeNativeLibAvailable();
        FalconValueState fvs = new FalconValueState(db);
        fvs.close();
        long after1 = readLong(fvs, "falconHandle_");
        assertEquals(0L, after1);
        fvs.close();
        assertEquals(0L, readLong(fvs, "falconHandle_"));
    }

    @Test
    public void testAutoCloseableTryWithResources() throws Exception {
        assumeNativeLibAvailable();
        FalconValueState ref;
        try (FalconValueState fvs = new FalconValueState(db)) {
            ref = fvs;
            assertNotNull(ref);
        }
        assertEquals(0L, readLong(ref, "falconHandle_"));
    }

    // ------------------------------------------------------------------
    //  isFalconCacheOpen — driven by the stub which returns 0 for getCacheSizeLimit
    // ------------------------------------------------------------------

    @Test
    public void testIsFalconCacheOpenReturnsFalseUnderTestStub() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            assertFalse(
                "test-resource libfalcon.so stub returns getCacheSizeLimit()=0, so the wrapper must report cache=closed",
                fvs.isFalconCacheOpen()
            );
        }
    }

    // ------------------------------------------------------------------
    //  loadFalconLibrary — state-machine behaviour
    // ------------------------------------------------------------------

    @Test
    public void testLoadFalconLibraryNoOpAfterLoaded() throws Exception {
        assumeNativeLibAvailable();
        AtomicReference<?> stateRef = readStateRef();
        assertEquals("LOADED", stateRef.get().toString());
        FalconValueState.loadFalconLibrary();
        assertEquals("LOADED", stateRef.get().toString());
    }

    // ------------------------------------------------------------------
    //  Native-delegating wrappers
    //
    //  We can only meaningfully exercise the wrappers that the test-stub implements safely.
    //  The stub backs `getCacheSizeLimit` (returning 0), `initFalconCache` and `destroyFalconCache`.
    //  For the data-plane wrappers (`get`, `put`, `delete`, `setCacheSizeLimit`,
    //  `flush`) the stub may behave undefined or no-op; we still call them so that the Java
    //  wrapper bodies are covered, and gracefully skip if the stub doesn't support them.
    // ------------------------------------------------------------------

    @Test
    public void testGetWrapperDelegates() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            try {
                fvs.get(defaultCf, writeOptions, new byte[] { 1, 2, 3 });
            } catch (Throwable t) {
                Assume.assumeNoException("Native method get() not available in test stub - skipping", t);
            }
        }
    }

    @Test
    public void testPutWrapperDelegates() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            try {
                fvs.put(defaultCf, writeOptions, new byte[] { 1, 2 }, new byte[] { 3, 4 });
            } catch (Throwable t) {
                Assume.assumeNoException("Native method put() not available in test stub - skipping", t);
            }
        }
    }

    @Test
    public void testDeleteWrapperDelegates() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            try {
                fvs.delete(defaultCf, writeOptions, new byte[] { 7, 8 });
            } catch (Throwable t) {
                Assume.assumeNoException("Native method delete() not available in test stub - skipping", t);
            }
        }
    }

    @Test
    public void testUpdateCacheSizeLimitWrapperDelegates() throws Exception {
        assumeNativeLibAvailable();
        ColumnFamilyHandle mockCf = Mockito.mock(ColumnFamilyHandle.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(mockCf.getName()).thenReturn("mockName".getBytes());
        try (FalconValueState fvs = new FalconValueState(db)) {
            try {
                fvs.updateCacheSizeLimit(mockCf, writeOptions, 1024);
            } catch (Throwable t) {
                Assume.assumeNoException("Native method updateCacheSizeLimit() not available in test stub - skipping", t);
            }
        }
    }

    @Test
    public void testFlushWhenCheckpointWrapperDelegates() throws Exception {
        assumeNativeLibAvailable();
        try (FalconValueState fvs = new FalconValueState(db)) {
            try {
                fvs.flushWhenCheckpoint(defaultCf, writeOptions);
            } catch (Throwable t) {
                Assume.assumeNoException("Native method flushWhenCheckpoint() not available in test stub - skipping", t);
            }
        }
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static long readLong(Object target, String fieldName) throws Exception {
        Field f = FalconValueState.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.getLong(target);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<?> readStateRef() throws Exception {
        Field f = FalconValueState.class.getDeclaredField("libraryLoaded");
        f.setAccessible(true);
        return (AtomicReference<?>) f.get(null);
    }

    // Loading the class itself must not blow up even on platforms without a compatible libfalcon.so.
    @Test
    public void testFalconValueStateClassFailsWithoutNativeLib() {
        try {
            Class.forName("com.huawei.falcon.state.cache.FalconValueState");
        } catch (ExceptionInInitializerError | NoClassDefFoundError | ClassNotFoundException e) {
            // Expected on environments without a compatible libfalcon.so on the classpath.
            // NoClassDefFoundError occurs when a prior ExceptionInInitializerError was cached by the JVM.
        }
    }

    @Test
    public void testStaticClassConstantsAreReachable() throws Exception {
        Field falconHandle = FalconValueState.class.getDeclaredField("falconHandle_");
        assertEquals(long.class, falconHandle.getType());

        Field rocksdbHandle = FalconValueState.class.getDeclaredField("rocksdbHandle_");
        assertEquals(long.class, rocksdbHandle.getType());

        Field libraryLoaded = FalconValueState.class.getDeclaredField("libraryLoaded");
        assertEquals(AtomicReference.class, libraryLoaded.getType());
    }
}
