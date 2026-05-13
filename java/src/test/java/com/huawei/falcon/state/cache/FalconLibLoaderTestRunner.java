package com.huawei.falcon.state.cache;

/**
 * Standalone entry point for testing FalconLibLoader failure paths.
 * Executed as a subprocess with a classpath that excludes libfalcon.so.
 */
public class FalconLibLoaderTestRunner {
    public static void main(String[] args) throws Exception {
        String method = args[0];
        FalconLibLoader loader = FalconLibLoader.getInstance();
        try {
            switch (method) {
                case "loadLibrary":
                    loader.loadLibrary();
                    System.out.println("SUCCESS (unexpected)");
                    break;
                case "loadLibraryFromJar":
                    loader.loadLibraryFromJar();
                    System.out.println("SUCCESS (unexpected)");
                    break;
                case "loadLibraryFromJarToTemp":
                    loader.loadLibraryFromJarToTemp();
                    System.out.println("SUCCESS (unexpected)");
                    break;
            }
        } catch (Throwable t) {
            System.out.println(t.getClass().getName() + ": " + t.getMessage());
        }
    }
}
