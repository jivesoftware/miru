package com.jivesoftware.os.miru.service.locator;

import java.io.File;

public class FileUtil {
    public static Exception remove(File _remove) {
        try {
            if (_remove.isDirectory()) {
                File[] array = _remove.listFiles();
                if (array != null) {
                    for (File array1 : array) {
                        remove(array1);
                    }
                }
            }
            _remove.delete();
            return null;
        } catch (Exception x) {
            return x;
        }
    }

    private FileUtil() {
    }
}
