/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.service.query.merge;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan
 */
public interface MiruResultMerger<R> {

    R merge(Optional<R> last, R current);

    R done(Optional<R> last, R alternative);

}

