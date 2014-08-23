package com.jivesoftware.os.miru.test;

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class MiruTestCollectionBackedFeatureSupplier implements MiruTestFeatureSupplier {

    private final Random random;
    private final float publicContainerPercent;

    private final MiruSchema miruSchema;
    private final MiruTenantId miruTenantId;
    private final TenantId tenantId;

    private final List<Id> users;
    private final List<Id> containers;
    private final List<Id> contentItems;
    private final List<Id> comments;

    private final WeightedRandomNumber participation;

    private final String globalAuthz;

    private final SetMultimap<Id, String> userAuthz;
    private final Map<Id, Id> contentAuthor;
    private final Map<Id, Id> containerGroup;
    private final Map<Id, Collection<Id>> contentItemContainers;
    private final Map<Id, Id> commentContentItem;

    private final AtomicLong idSequence = new AtomicLong();
    private final TimestampSequence timestampSequence;

    public MiruTestCollectionBackedFeatureSupplier(Random random, float publicContainerPercent, String tenant,
            int preUsers, int preContainers, int preContentItems, int preComments, WeightedRandomNumber participation, int totalActivities) {

        this.random = random;
        this.publicContainerPercent = publicContainerPercent;

        this.miruSchema = new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA);
        this.miruTenantId = new MiruTenantId(tenant.getBytes(Charsets.UTF_8));
        this.tenantId = new TenantId(tenant);

        this.users = Lists.newArrayList(generateIds(Math.max(preUsers, 1)));
        this.containers = Lists.newArrayList(generateIds(Math.max(preContainers, 1)));
        this.contentItems = Lists.newArrayList(generateIds(Math.max(preContentItems, 1)));
        this.comments = Lists.newArrayList(generateIds(Math.max(preComments, 1)));

        this.participation = participation;

        this.globalAuthz = generateId().toStringForm();
        this.userAuthz = HashMultimap.create();
        this.contentAuthor = Maps.newHashMap();
        this.containerGroup = Maps.newHashMap();
        this.contentItemContainers = Maps.newHashMap();
        this.commentContentItem = Maps.newHashMap();

        for (Id id : this.containers) {
            this.containerGroup.put(id, generateId());
        }

        for (Id id : this.contentItems) {
            Id userId = oldUsers(1).get(0);
            this.contentAuthor.put(id, userId);
            List<Id> containerIds = oldContainers(2);
            this.contentItemContainers.put(id, containerIds);
            this.userAuthz.putAll(userId, Arrays.asList(authz(containerIds.toArray(new Id[containerIds.size()]))));
        }

        for (Id id : this.comments) {
            Id userId = oldUsers(1).get(0);
            this.contentAuthor.put(id, userId);
            Id contentItemId = oldContentItem();
            Collection<Id> containerIds = containersForContentItem(contentItemId);
            this.commentContentItem.put(id, contentItemId);
            this.userAuthz.putAll(userId, Arrays.asList(authz(containerIds.toArray(new Id[containerIds.size()]))));
        }

        this.timestampSequence = new TimestampSequence(totalActivities, 5_000); //TODO config millis between activity
    }

    private Id generateId() {
        return new Id(idSequence.incrementAndGet());
    }

    private List<Id> generateIds(int numIds) {
        List<Id> ids = Lists.newArrayListWithCapacity(numIds);
        for (int i = 0; i < numIds; i++) {
            ids.add(generateId());
        }
        return ids;
    }

    @Override
    public MiruSchema miruSchema() {
        return miruSchema;
    }

    @Override
    public TenantId tenantId() {
        return tenantId;
    }

    @Override
    public MiruTenantId miruTenantId() {
        return miruTenantId;
    }

    @Override
    public String[] authz(Id... containerIds) {
        Set<String> authzs = Sets.newHashSet();
        int modRemainder = (int) (publicContainerPercent * 100);
        for (Id containerId : containerIds) {
            // a pretty hacky fakery of authz
            if (containerId.hashCode() % 100 < modRemainder) {
                authzs.add(globalAuthz);
            }
            authzs.add(groupForContainer(containerId).toStringForm());
        }
        return authzs.toArray(new String[0]);
    }

    @Override
    public String[] globalAuthz() {
        return new String[]{globalAuthz};
    }

    @Override
    public Set<String> userAuthz(Id userId) {
        Set<String> authz = Sets.newHashSet(userAuthz.get(userId));
        authz.add(globalAuthz);
        return authz;
    }

    @Override
    public List<Id> oldUsers(int count) {
        if (users.size() < count) {
            throw new IllegalStateException("too few users");
        }
        Set<Id> ids = Sets.newHashSet();
        while (ids.size() < count) {
            ids.add(users.get(random.nextInt(users.size())));
        }
        return Lists.newArrayList(ids);
    }

    @Override
    public int numParticipants() {
        return (int) participation.get(random);
    }

    @Override
    public int numUserMentions() {
        final int maxUserMentions = 5; //TODO config
        return (int) new OneTailedRandomNumber(2, maxUserMentions, 0, maxUserMentions).get(random);
    }

    @Override
    public int numContainerMentions() {
        final int maxContainerMentions = 3; //TODO config
        return (int) new OneTailedRandomNumber(8, maxContainerMentions, 0, maxContainerMentions).get(random);
    }

    @Override
    public Id contentAuthor(Id contentItemId) {
        return checkNotNull(contentAuthor.get(contentItemId));
    }

    @Override
    public Id newContainer() {
        Id id = generateId();
        containers.add(id);
        containerGroup.put(id, generateId());
        return id;
    }

    @Override
    public List<Id> oldContainers(int count) {
        if (containers.size() < count) {
            throw new IllegalStateException("too few containers");
        }
        Set<Id> ids = Sets.newHashSet();
        while (ids.size() < count) {
            ids.add(containers.get(random.nextInt(containers.size())));
        }
        return Lists.newArrayList(ids);
    }

    @Override
    public Id groupForContainer(Id containerId) {
        return checkNotNull(containerGroup.get(containerId), "Invalid containerId");
    }

    @Override
    public Collection<Id> containersForContentItem(Id contentItemId) {
        return checkNotNull(contentItemContainers.get(contentItemId), "Invalid contentItemId");
    }

    @Override
    public Id newContentItem(Id authorId, Id... containerIds) {
        Id id = generateId();
        contentItems.add(id);
        contentAuthor.put(id, authorId);
        contentItemContainers.put(id, Arrays.asList(containerIds));
        return id;
    }

    @Override
    public Id oldContentItem() {
        return contentItems.get(random.nextInt(contentItems.size()));
    }

    @Override
    public Id newContentVersion(Id contentItemId) {
        return generateId();
    }

    @Override
    public Id newComment(Id authorId, Id contentItemId) {
        Id id = generateId();
        comments.add(id);
        contentAuthor.put(id, authorId);
        commentContentItem.put(id, contentItemId);
        return id;
    }

    @Override
    public Id newCommentVersion(Id commentId) {
        return generateId();
    }

    @Override
    public Id oldComment() {
        return comments.get(random.nextInt(comments.size()));
    }

    @Override
    public Id contentItemForComment(Id commentId) {
        return commentContentItem.get(commentId);
    }

    @Override
    public long nextTimestamp() {
        return timestampSequence.next();
    }

    @Override
    public long lastTimestamp() {
        return timestampSequence.last();
    }

    private static class TimestampSequence {

        private final long millisBetweenTimestamps;
        private final AtomicLong timestamp;
        private final IdPacker idPacker;

        private TimestampSequence(int totalActivities, long millisBetweenTimestamps) {
            this.millisBetweenTimestamps = millisBetweenTimestamps;
            this.timestamp = new AtomicLong(System.currentTimeMillis() - totalActivities * millisBetweenTimestamps);
            this.idPacker = new SnowflakeIdPacker();
        }

        public long next() {
            return idPacker.pack(timestamp.addAndGet(millisBetweenTimestamps), 0, 0);
        }

        public long last() {
            return idPacker.pack(timestamp.get(), 0, 0);
        }
    }
}
