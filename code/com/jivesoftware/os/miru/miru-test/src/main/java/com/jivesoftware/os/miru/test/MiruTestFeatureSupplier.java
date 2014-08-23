package com.jivesoftware.os.miru.test;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.miru.api.activity.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
*
*/
public interface MiruTestFeatureSupplier {

    MiruSchema miruSchema();

    TenantId tenantId();

    MiruTenantId miruTenantId();

    String[] authz(Id... containerIds);

    String[] globalAuthz();

    Set<String> userAuthz(Id userId);

    List<Id> oldUsers(int count);

    int numParticipants();

    int numUserMentions();

    int numContainerMentions();

    Id contentAuthor(Id contentItemId);

    Id newContainer();

    List<Id> oldContainers(int count);

    Id groupForContainer(Id containerId);

    Collection<Id> containersForContentItem(Id contentItem);

    Id newContentItem(Id authorId, Id... containerIds);

    Id oldContentItem();

    Id newContentVersion(Id contentItemId);

    Id newComment(Id authorId, Id contentItemId);

    Id newCommentVersion(Id commentId);

    Id oldComment();

    Id contentItemForComment(Id commentId);

    long nextTimestamp();

    long lastTimestamp();
}
