package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 *
 */
public class MiruLowestLatencySolverTest {

    @Test
    public void testSolveForLowestLatency() throws Exception {
        Executor executor = Executors.newFixedThreadPool(10);
        int initialSolvers = 2;
        int maxNumberOfSolvers = 10;
        long addAnotherSolverAfterNMillis = 100;
        long failAfterNMillis = 3000;

        MiruLowestLatencySolver solver = new MiruLowestLatencySolver(executor, initialSolvers, maxNumberOfSolvers,
            addAnotherSolverAfterNMillis, failAfterNMillis);

        ImmutableList.Builder<MiruSolvable<Integer>> solvableBuilder = ImmutableList.<MiruSolvable<Integer>>builder();
        for (int i = 0; i < 10; i++) {
            final int id = i;
            solvableBuilder.add(new MiruSolvable<>(
                new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(1), new MiruHost("localhost", 49600)),
                new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        Thread.sleep(id * 1000); // Fake latency for each callable, 0 should always win
                        return id;
                    }
                }));
        }
        List<MiruSolvable<Integer>> solvables = Lists.newArrayList(solvableBuilder.build());
        Collections.shuffle(solvables); // randomize the solvers

        Integer solution = solver.solve(solvables.iterator(), Optional.<Long>absent()).getResult();
        assertNotNull(solution, "The solution was null, this probably means that the solver timed out when it shouldn't have.");
        assertEquals((int) solution, 0);
    }
}
