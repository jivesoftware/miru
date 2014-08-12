package com.jivesoftware.os.miru.client;

import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import java.util.List;

public interface MiruClient {

    void sendActivity(List<MiruActivity> activities, boolean recoverFromRemoval) throws Exception;

    void removeActivity(List<MiruActivity> activities) throws Exception;

    void sendRead(MiruReadEvent readEvent) throws Exception;

    void sendUnread(MiruReadEvent readEvent) throws Exception;

    void sendAllRead(MiruReadEvent readEvent) throws Exception;

}
