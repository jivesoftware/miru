/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.wal.Sip;
import java.io.IOException;

/**
 * @author jonathan
 */
public interface MiruSipIndex {

    Sip getSip() throws IOException;

    boolean setSip(Sip sip) throws IOException;

}
