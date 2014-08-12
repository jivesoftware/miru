package com.jivesoftware.os.miru.api.query.filter;

/**
 * http://en.wikipedia.org/wiki/Truth_table
 *
 * @author jonathan
 */
public enum MiruFilterOperation {

    and, // currently supported
    nand, // TODO add support
    or, // currently supported
    nor, // TODO add support
    xor, // currently supported
    xnor, // TODO add support
    pButNotQ, // currently supported
    ifThen, // TODO add support
    thenif, // TODO add support
    notPButQ // TODO add support

}
