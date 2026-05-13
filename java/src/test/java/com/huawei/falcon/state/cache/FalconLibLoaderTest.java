package com.huawei.falcon.state.cache;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assume;
import org.junit.Test;

public class FalconLibLoaderTest {

    @Test
    public void testGetInstanceReturnsSameInstance() {
        FalconLibLoader instance1 = FalconLibLoader.getInstance();
        FalconLibLoader instance2 = FalconLibLoader.getInstance();
        assertSame(instance1, instance2);
    }

    @Test
    public void testLoadLibraryIsIdempotentAfterFirstCall() throws IOException {
        FalconLibLoader loader = FalconLibLoader.getInstance();
        // First call loads the native library (or is a no-op if already loaded by another test).
        // Second call must be a no-op — initialized flag prevents re-loading.
        try {
            loader.loadLibrary();
        } catch (UnsatisfiedLinkError e) {
            // Native library not compatible with this platform (e.g. Linux .so on Windows).
            // Skip the test gracefully rather than failing.
            Assume.assumeNoException("Native libfalcon not available on this platform - skipping", e);
        }
        loader.loadLibrary();
        // No exception means the initialized guard works correctly.
    }

    // --- Negative-path tests: libfalcon.so deliberately absent from the loader's classpath. ---
    //
    // FalconLibLoader resolves libfalcon.so via its own ClassLoader's getResourceAsStream.
    // Production tests stage libfalcon.so under target/test-classes, which makes the
    // negative paths unreachable for the singleton already loaded by other tests. We work
    // around that by re-loading FalconLibLoader inside an isolated URLClassLoader whose
    // search path excludes target/test-classes (the only place libfalcon.so lives).

    @Test
    public void testLoadLibraryThrowsWhenNativeNotAvailable() throws Exception {
        Object loader = freshLoaderWithoutLibfalcon();
        Method loadLibrary = loader.getClass().getDeclaredMethod("loadLibrary");
        try {
            loadLibrary.invoke(loader);
            fail("Should have thrown an exception");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue(
                "Expected RuntimeException or IOException, got: " + cause,
                cause instanceof RuntimeException || cause instanceof IOException);
        }
    }

    @Test
    public void testLoadLibraryFromJarFailsWhenNotInJar() throws Exception {
        Object loader = freshLoaderWithoutLibfalcon();
        Method loadLibraryFromJar = loader.getClass().getDeclaredMethod("loadLibraryFromJar");
        loadLibraryFromJar.setAccessible(true);
        try {
            loadLibraryFromJar.invoke(loader);
            fail("Should have thrown an exception");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue(
                "Expected RuntimeException or IOException, got: " + cause,
                cause instanceof RuntimeException || cause instanceof IOException);
        }
    }

    @Test
    public void testLoadLibraryFromJarToTempFailsOnResource() throws Exception {
        Object loader = freshLoaderWithoutLibfalcon();
        Method loadLibraryFromJarToTemp =
            loader.getClass().getDeclaredMethod("loadLibraryFromJarToTemp");
        loadLibraryFromJarToTemp.setAccessible(true);
        try {
            loadLibraryFromJarToTemp.invoke(loader);
            fail("Should have thrown RuntimeException");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue("Expected RuntimeException, got: " + cause, cause instanceof RuntimeException);
            assertTrue(
                "Message should mention libfalcon.so missing, got: " + cause.getMessage(),
                cause.getMessage().contains("libfalcon.so was not found inside JAR"));
        }
    }

    /**
     * Build a child ClassLoader whose search path is {@code target/classes} only — i.e.
     * deliberately excludes {@code target/test-classes}, where libfalcon.so is staged for
     * other tests. Then load FalconLibLoader inside it and return its singleton.
     *
     * <p>Parent is the platform ClassLoader so production classes (Flink, RocksDB,
     * java.*) still resolve, but {@code getResourceAsStream("libfalcon.so")} on the
     * isolated FalconLibLoader Class returns null.
     */
    private static Object freshLoaderWithoutLibfalcon() throws Exception {
        Path mainClasses = locateMainClassesDir();
        URL[] urls = new URL[] { mainClasses.toUri().toURL() };
        URLClassLoader isolated = new URLClassLoader(urls, ClassLoader.getSystemClassLoader().getParent());
        Class<?> isolatedLoaderCls = Class.forName(
            "com.huawei.falcon.state.cache.FalconLibLoader", true, isolated);
        // Sanity: the isolated FalconLibLoader Class must NOT see libfalcon.so on its classpath.
        if (isolatedLoaderCls.getClassLoader().getResource("libfalcon.so") != null) {
            throw new IllegalStateException(
                "Test setup invalid: isolated classloader still sees libfalcon.so");
        }
        Method getInstance = isolatedLoaderCls.getDeclaredMethod("getInstance");
        return getInstance.invoke(null);
    }

    /**
     * Locate the project's compiled main classes directory. Surefire runs with the module
     * directory as cwd, so {@code target/classes} resolves to the right place.
     */
    private static Path locateMainClassesDir() {
        Path candidate = Paths.get("target", "classes").toAbsolutePath();
        if (Files.isDirectory(candidate)) {
            return candidate;
        }
        // Fallback: derive it from this test class's location on disk.
        try {
            URL self = FalconLibLoaderTest.class
                .getProtectionDomain().getCodeSource().getLocation();
            File testClasses = new File(self.toURI());          // .../target/test-classes
            File mainClasses = new File(testClasses.getParentFile(), "classes");
            if (mainClasses.isDirectory()) {
                return mainClasses.toPath();
            }
        } catch (Exception ignored) {
            // fall through
        }
        throw new IllegalStateException(
            "Could not locate target/classes — tried " + candidate);
    }
}
