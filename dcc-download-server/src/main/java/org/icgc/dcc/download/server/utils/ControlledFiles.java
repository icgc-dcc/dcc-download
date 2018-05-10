package org.icgc.dcc.download.server.utils;

public final class ControlledFiles {
    public static boolean isControlled(String filePath) {
        return filePath.contains("controlled");
    }
}
