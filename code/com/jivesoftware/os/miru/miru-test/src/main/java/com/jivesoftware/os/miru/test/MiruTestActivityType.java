package com.jivesoftware.os.miru.test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
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
            ObjectId verbSubject = new ObjectId("Place", containerId);
            String[] authz = supplier.authz(containerId);
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), "SocialNewsParent_ABC")
                    .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), "SocialNewsContainer_ABC")
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "PlaceActivitySearchView")
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
            Id contentItemId = supplier.newContentItem(authorId, containerIds.toArray(new Id[containerIds.size()]));
            ObjectId verbSubject = new ObjectId("PostVersion", supplier.newContentVersion(contentItemId));
            String[] authz = supplier.authz(containerIds.toArray(new Id[containerIds.size()]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), contentItemId.toStringForm())
                    .putAllFieldValues(MiruFieldName.CONTAINER_IDS.getFieldName(), Lists.transform(containerIds, ID_TO_STRING_FORM))
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putAllFieldValues(MiruFieldName.PARTICIPANT_IDS.getFieldName(), Lists.transform(users.subList(0, participants), ID_TO_STRING_FORM))
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "ContentVersionActivitySearchView")
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
            ObjectId verbSubject = new ObjectId("CommentVersion", supplier.newCommentVersion(commentId));
            String[] authz = supplier.authz(containerIds.toArray(new Id[containerIds.size()]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), contentItemId.toStringForm())
                    .putAllFieldValues(MiruFieldName.CONTAINER_IDS.getFieldName(), Collections2.transform(containerIds, ID_TO_STRING_FORM))
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putAllFieldValues(MiruFieldName.PARTICIPANT_IDS.getFieldName(), Lists.transform(users.subList(0, participants), ID_TO_STRING_FORM))
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "CommentVersionActivitySearchView")
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
            ObjectId author = new ObjectId("User", authorId);
            Id streamId = CompositeId.createOrdered("ConnectionsStream", supplier.tenantId(), author.toStringForm());
            ObjectId stream = new ObjectId("ConnectionsActivityStream", streamId);
            Id followedUserId = users.get(1);
            ObjectId followedUser = new ObjectId("User", followedUserId);
            Id userFollowId = CompositeId.createOrdered("Follow", supplier.tenantId(), author.toStringForm(), followedUser.toStringForm(),
                    stream.toStringForm());

            ObjectId verbSubject = new ObjectId("UserFollow", userFollowId);
            String[] authz = supplier.globalAuthz();
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), "SocialNewsParent_ABC")
                    .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), "SocialNewsContainer_ABC")
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "UserFollowActivitySearchView")
                    .build();
            return activity;
        }
    }),
    joinPlace(new ActivityGenerator() {
        @Override
        public MiruActivity generate(MiruTestFeatureSupplier supplier, long time) {
            Id authorId = supplier.oldUsers(1).get(0);
            ObjectId author = new ObjectId("User", authorId);
            Id containerId = supplier.oldContainers(1).get(0);
            ObjectId group = new ObjectId("Group", supplier.groupForContainer(containerId));
            Id membershipId = CompositeId.createOrdered("Membership", supplier.tenantId(), group.toStringForm(), author.toStringForm());

            ObjectId verbSubject = new ObjectId("Membership", membershipId);
            String[] authz = supplier.authz(containerId);
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), "SocialNewsParent_ABC")
                    .putFieldValue(MiruFieldName.CONTAINER_ID.getFieldName(), "SocialNewsContainer_ABC")
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "MembershipActivitySearchView")
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
            ObjectId contentItem = new ObjectId("Post", contentItemId);
            Id likeId = CompositeId.createOrdered("Like", supplier.tenantId(), authorId.toStringForm(), contentItem.toStringForm());
            ObjectId verbSubject = new ObjectId("Like", likeId);
            Collection<Id> containerIds = supplier.containersForContentItem(contentItemId);
            String[] authz = supplier.authz(containerIds.toArray(new Id[containerIds.size()]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), "AcclaimParent_ABC")
                    .putFieldValue(MiruFieldName.CONTAINER_IDS.getFieldName(), "AcclaimContainer_ABC")
                    .putFieldValue(MiruFieldName.META_ID.getFieldName(), contentItemId.toStringForm())
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putFieldValue(MiruFieldName.PARTICIPANT_IDS.getFieldName(), ID_TO_STRING_FORM.apply(contentAuthorId))
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "LikeActivitySearchView")
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
            ObjectId comment = new ObjectId("Comment", commentId);
            Id likeId = CompositeId.createOrdered("Like", supplier.tenantId(), authorId.toStringForm(), comment.toStringForm());
            ObjectId verbSubject = new ObjectId("Like", likeId);
            Id contentItemId = supplier.contentItemForComment(commentId);
            Collection<Id> containerIds = supplier.containersForContentItem(contentItemId);
            String[] authz = supplier.authz(containerIds.toArray(new Id[containerIds.size()]));
            MiruActivity activity = new MiruActivity.Builder(supplier.miruTenantId(), time, authz, 0)
                    .putFieldValue(MiruFieldName.ACTIVITY_PARENT.getFieldName(), "AcclaimParent_ABC")
                    .putFieldValue(MiruFieldName.CONTAINER_IDS.getFieldName(), "AcclaimContainer_ABC")
                    .putFieldValue(MiruFieldName.META_ID.getFieldName(), commentId.toStringForm())
                    .putFieldValue(MiruFieldName.AUTHOR_ID.getFieldName(), authorId.toStringForm())
                    .putFieldValue(MiruFieldName.VERB_SUBJECT_CLASS_NAME.getFieldName(), verbSubject.getClassName())
                    .putFieldValue(MiruFieldName.PARTICIPANT_IDS.getFieldName(), ID_TO_STRING_FORM.apply(commentAuthorId))
                    .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), verbSubject.toStringForm())
                    .putFieldValue(MiruFieldName.VIEW_CLASS_NAME.getFieldName(), "LikeActivitySearchView")
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
