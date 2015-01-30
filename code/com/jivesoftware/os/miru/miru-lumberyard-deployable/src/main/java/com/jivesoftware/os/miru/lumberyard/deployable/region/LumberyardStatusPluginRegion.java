package com.jivesoftware.os.miru.lumberyard.deployable.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
// soy.miru.page.lumberyardsStatusPluginRegion
public class LumberyardStatusPluginRegion implements MiruPageRegion<Optional<LumberyardStatusPluginRegion.LumberyardStatusPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    
    public LumberyardStatusPluginRegion(String template,
        MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    public static class LumberyardStatusPluginRegionInput {

        final String foo;

        public LumberyardStatusPluginRegionInput(String foo) {

            this.foo = foo;
        }

    }

    @Override
    public String render(Optional<LumberyardStatusPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                LumberyardStatusPluginRegionInput input = optionalInput.get();
                

                data.put("status", Arrays.asList(new String[]{
                "Todo build up lumberyard stats:",
                "i.e ingressed:BLA"}));
                
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Analytics";
    }

    public static enum Type {

        viewed(0),
        created(1),
        modified(2),
        commented(3),
        replied(4),
        voted(5),
        completed(6),
        updatedStatus(7),
        bookmarked(8),
        rated(9),
        blank(10),
        liked(11),
        joined(12),
        connected(13),
        followed(14),
        unfollowed(15),
        read(16),
        shared(17),
        NOTFOUND(18),
        UNAUTHORIZED(19),
        mentioned(20),
        promoted(21),
        clicked(22),
        logged_in(23),
        logged_out(24),
        applied(25),
        removed(26),
        repost(27),
        object_exclusion_added(28),
        object_exclusion_removed(29),
        context_exclusion_added(30),
        context_exclusion_removed(31),
        user_deleted(32),
        unread(33),
        register_database(34),
        manage(35),
        unmanage(36),
        tracked(37),
        untracked(38),
        allread(39),
        inUserStream(40),
        inUserInBox(41),
        inUserActivityQueue(42),
        unliked(43),
        projectCompleted(44),
        disinterest(45),
        notification(46),
        watch(47),
        unwatch(48),
        dismiss(49),
        unconnected(50),
        reshred_complete(51),
        unjoined(52),
        trace(53),
        heartbeat(54),
        moved(55),
        repairFollowHint(56),
        search(57),
        user_search(58),
        object_untrack_added(59),
        object_untrack_removed(60),
        digest(61),
        correct_answer_set(62),
        correct_answer_removed(63),
        tagged(64),
        outcome_set(65),
        outcome_removed(66),
        object_deleted(67),
        outcomes_modified(68),
        acclaim_added(69),
        acclaim_removed(70),
        acclaim_modified(71);

        private static Map<Integer, Type> typeMap = new ConcurrentHashMap<>();

        static {
            for (Type type : values()) {
                typeMap.put(type.getID(), type);
            }
        }

        private int id;

        Type(int id) {
            this.id = id;
        }

        public int getID() {
            return id;
        }

        public static Type valueOf(int id) {
            return typeMap.get(id);
        }
    }
}
