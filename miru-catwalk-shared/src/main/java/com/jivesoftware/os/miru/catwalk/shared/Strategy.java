package com.jivesoftware.os.miru.catwalk.shared;

/**
 *
 */
public enum Strategy {
    UNIT_WEIGHTED, // S = mean(A,B,C,D)
    REGRESSION_WEIGHTED, // S = 0.5*A + 0.4*B + 0.4*C + 0.3*D
    MAX
}
