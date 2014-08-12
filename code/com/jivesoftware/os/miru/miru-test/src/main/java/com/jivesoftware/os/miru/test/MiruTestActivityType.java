package com.jivesoftware.os.miru.test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.jive.platform.model.model.api.Ref;
import com.jivesoftware.jive.ui.base.model.event.Comment;
import com.jivesoftware.jive.ui.base.model.event.CommentVersion;
import com.jivesoftware.jive.ui.base.model.event.ConnectionsActivityStream;
import com.jivesoftware.jive.ui.base.model.event.Group;
import com.jivesoftware.jive.ui.base.model.event.Like;
import com.jivesoftware.jive.ui.base.model.event.Membership;
import com.jivesoftware.jive.ui.base.model.event.Place;
import com.jivesoftware.jive.ui.base.model.event.Post;
import com.jivesoftware.jive.ui.base.model.event.PostVersion;
import com.jivesoftware.jive.ui.base.model.event.User;
import com.jivesoftware.jive.ui.base.model.event.UserFollow;
import com.jivesoftware.jive.ui.base.model.id.CompositeIds;
import com.jivesoftware.jive.ui.base.model.id.WellKnownObjectIds;
import com.jivesoftware.jive.ui.base.model.view.CommentVersionActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.ContentVersionActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.LikeActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.MembershipActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.PlaceActivitySearchView;
import com.jivesoftware.jive.ui.base.model.view.UserFollowActivitySearchView;
import com.jivesoftware.os.jive.utils.id.Id;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public enum MiruTestActivityType {

    createPlace(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            Id authorId = supplier.oldUsers(1).get(0);
            Id containerId = supplier.newContainer();
            Ref<Place> verbSubject = Ref.fromId(containerId, Place.class);
            String[] authz = supplier.authz(containerId);
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), WellKnownObjectIds.getSocialNewsParent(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), WellKnownObjectIds.getSocialNewsContainer(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), PlaceActivitySearchView.class.getSimpleName())
                .build();
            return activity;
        }
    }),

    contentItem(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            int userMentions = supplier.numUserMentions();
            int containerMentions = supplier.numContainerMentions();
            int participants = supplier.numParticipants();
            List<Id> users = supplier.oldUsers(1 + Math.max(participants, userMentions));
            List<Id> containerIds = supplier.oldContainers(2);
            Id authorId = users.get(0);
            Id contentItemId = supplier.newContentItem(authorId, containerIds.toArray(new Id[0]));
            Ref<PostVersion> verbSubject = Ref.fromId(supplier.newContentVersion(contentItemId), PostVersion.class);
            String[] authz = supplier.authz(containerIds.toArray(new Id[0]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), contentItemId.toStringForm())
                .putAllFieldValues(MiruFieldName.CONTAINER_IDS.getFieldName(), Lists.transform(containerIds, ID_TO_STRING_FORM))
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putAllFieldValues(MiruFieldName.PARTICIPANT_IDS.getFieldName(), Lists.transform(users.subList(0, participants), ID_TO_STRING_FORM))
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), ContentVersionActivitySearchView.class.getSimpleName())
                .putAllFieldValues(MiruFieldName.MENTIONED_USER_IDS.getFieldName(), Lists.transform(users.subList(1, 1 + userMentions), ID_TO_STRING_FORM))
                .putAllFieldValues(MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName(),
                    Lists.transform(supplier.oldContainers(containerMentions), ID_TO_STRING_FORM))
                .build();
            return activity;
        }
    }),

    comment(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            int userMentions = supplier.numUserMentions();
            int containerMentions = supplier.numContainerMentions();
            int participants = supplier.numParticipants();
            List<Id> users = supplier.oldUsers(1 + Math.max(participants, userMentions));
            Id authorId = users.get(0);
            Id contentItemId = supplier.oldContentItem();
            Id commentId = supplier.newComment(authorId, contentItemId);
            Collection<Id> containerIds = supplier.containersForContentItem(contentItemId);
            Ref<CommentVersion> verbSubject = Ref.fromId(supplier.newCommentVersion(commentId), CommentVersion.class);
            String[] authz = supplier.authz(containerIds.toArray(new Id[0]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), contentItemId.toStringForm())
                .putAllFieldValues(MiruFieldName.CONTAINER_IDS.getFieldName(), Collections2.transform(containerIds, ID_TO_STRING_FORM))
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putAllFieldValues(MiruFieldName.PARTICIPANT_IDS.getFieldName(), Lists.transform(users.subList(0, participants), ID_TO_STRING_FORM))
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), CommentVersionActivitySearchView.class.getSimpleName())
                .putAllFieldValues(MiruFieldName.MENTIONED_USER_IDS.getFieldName(), Lists.transform(users.subList(1, 1 + userMentions), ID_TO_STRING_FORM))
                .putAllFieldValues(MiruFieldName.MENTIONED_CONTAINER_IDS.getFieldName(),
                    Lists.transform(supplier.oldContainers(containerMentions), ID_TO_STRING_FORM))
                .build();
            return activity;
        }
    }),

    userFollow(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            List<Id> users = supplier.oldUsers(2);
            Id authorId = users.get(0);
            Ref<User> author = Ref.fromId(authorId, User.class);
            Id streamId = CompositeIds.connectionsStreamId(supplier.tenantId(), author);
            Ref<ConnectionsActivityStream> stream = Ref.fromId(streamId, ConnectionsActivityStream.class);
            Id followedUserId = users.get(1);
            Ref<User> followedUser = Ref.fromId(followedUserId, User.class);
            Id userFollowId = CompositeIds.followId(supplier.tenantId(), author, followedUser, stream);
            Ref<UserFollow> verbSubject = Ref.fromId(userFollowId, UserFollow.class);
            String[] authz = supplier.globalAuthz();
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), WellKnownObjectIds.getSocialNewsParent(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), WellKnownObjectIds.getSocialNewsContainer(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), UserFollowActivitySearchView.class.getSimpleName())
                .build();
            return activity;
        }
    }),

    joinPlace(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            Id authorId = supplier.oldUsers(1).get(0);
            Ref<User> author = Ref.fromId(authorId, User.class);
            Id containerId = supplier.oldContainers(1).get(0);
            Ref<Group> group = Ref.fromId(supplier.groupForContainer(containerId), Group.class);
            Id membershipId = CompositeIds.membershipId(supplier.tenantId(), group, author);
            Ref<Membership> verbSubject = Ref.fromId(membershipId, Membership.class);
            String[] authz = supplier.authz(containerId);
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), WellKnownObjectIds.getSocialNewsParent(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), WellKnownObjectIds.getSocialNewsContainer(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), MembershipActivitySearchView.class.getSimpleName())
                .build();
            return activity;
        }
    }),

    likeContentItem(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            Id authorId = supplier.oldUsers(1).get(0);
            Id contentItemId = supplier.oldContentItem();
            Id contentAuthorId = supplier.contentAuthor(contentItemId);
            Ref<Post> contentItem = Ref.fromId(contentItemId, Post.class);
            Id likeId = CompositeIds.likeId(supplier.tenantId(), authorId, contentItem.getObjectId());
            Ref<Like> verbSubject = Ref.fromId(likeId, Like.class);
            Collection<Id> containerIds = supplier.containersForContentItem(contentItemId);
            String[] authz = supplier.authz(containerIds.toArray(new Id[0]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), WellKnownObjectIds.getAcclaimParent(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.CONTAINER_IDS.getFieldName(), WellKnownObjectIds.getAcclaimContainer(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.META_ID.getFieldName(), contentItemId.toStringForm())
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putFieldValue(MiruFieldName.PARTICIPANT_IDS.getFieldName(), ID_TO_STRING_FORM.apply(contentAuthorId))
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), LikeActivitySearchView.class.getSimpleName())
                .build();
            return activity;
        }
    }),

    likeComment(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            Id authorId = supplier.oldUsers(1).get(0);
            Id commentId = supplier.oldComment();
            Id commentAuthorId = supplier.contentAuthor(commentId);
            Ref<Comment> comment = Ref.fromId(commentId, Comment.class);
            Id likeId = CompositeIds.likeId(supplier.tenantId(), authorId, comment.getObjectId());
            Ref<Like> verbSubject = Ref.fromId(likeId, Like.class);
            Id contentItemId = supplier.contentItemForComment(commentId);
            Collection<Id> containerIds = supplier.containersForContentItem(contentItemId);
            String[] authz = supplier.authz(containerIds.toArray(new Id[0]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), WellKnownObjectIds.getAcclaimParent(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.CONTAINER_IDS.getFieldName(), WellKnownObjectIds.getAcclaimContainer(supplier.tenantId()).getId().toStringForm())
                .putFieldValue(MiruFieldName.META_ID.getFieldName(), commentId.toStringForm())
                .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getObjectId().getClassName())
                .putFieldValue(MiruFieldName.PARTICIPANT_IDS.getFieldName(), ID_TO_STRING_FORM.apply(commentAuthorId))
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.getObjectId().toStringForm())
                .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), LikeActivitySearchView.class.getSimpleName())
                .build();
            return activity;
        }
    });

    private final ActivityGenerator generator;

    private MiruTestActivityType(ActivityGenerator generator) {
        this.generator = generator;
    }

    public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
        return generator.generate(supplier, time);
    }

    private interface ActivityGenerator {

        MiruActivity generate(MiruTestFeatureSupplier supplier, long time);
    }

    private static Function<Id, String> ID_TO_STRING_FORM = new Function<Id, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Id input) {
            return input != null ? input.toStringForm() : null;
        }
    };
}
