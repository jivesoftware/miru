/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.plugin.index;

import java.io.IOException;

/**
 * @author jonathan
 */
public interface MiruSipIndex {

    long getSip() throws IOException;

    boolean setSip(long timestamp) throws IOException;
}
