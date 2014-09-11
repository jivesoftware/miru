package com.jivesoftware.os.miru.service.locator;

import java.io.File;

public class FileUtil {
    public static Exception remove(File _remove) {
        try {
            if (_remove.isDirectory()) {
                File[] array = _remove.listFiles();
                if (array != null) {
                    for (int i = 0; i < array.length; i++) {
                        remove(array[i]);
                    }
                }
            }
            _remove.delete();
            return null;
        } catch (Exception x) {
            return x;
        }
    }
}
