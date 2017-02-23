package com.jivesoftware.os.miru.api;

import com.google.common.base.Strings;

public class MiruHostProvider {

    public static MiruHost fromInstance(Integer instanceName, String instanceKey) {
        return new MiruHost(Strings.padStart(String.valueOf(instanceName), 5, '0') + "_" + instanceKey);
    }

    public static MiruHost fromHostPort(String host, int port) {
        return new MiruHost(host + ":" + port);
    }

    public static boolean checkEquals(MiruHost miruHost, int instanceName, String instanceKey, String host, int port) {
        String logicalName = miruHost.getLogicalName();
        if (logicalName.indexOf('_') > -1) {
            return miruHost.equals(fromInstance(instanceName, instanceKey));
        } else if (logicalName.indexOf(':') > -1) {
            return miruHost.equals(fromHostPort(host, port));
        } else {
            return false;
        }
    }

}
