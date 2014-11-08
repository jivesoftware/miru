/*
 * Copyright 2014 jonathan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.jive.utils.id.Id;
import java.util.Objects;

/**
 * Represents the full context of a user, including both the user itself, and the set of groups the user belongs to. This full context is passed along with
 * write and read requests to allow authorization checks.
 */
public class MiruActorId {

    public static final MiruActorId NOT_PROVIDED = new MiruActorId(new Id(Long.MAX_VALUE));

    private final Id actorId;

    @JsonCreator
    public MiruActorId(@JsonProperty("actorId") Id actorId) {
        Preconditions.checkNotNull(actorId);
        this.actorId = actorId;
    }

    public Id getActorId() {
        return actorId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MiruActorId other = (MiruActorId) obj;
        if (!Objects.equals(this.actorId, other.actorId)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.actorId);
        return hash;
    }

    @Override
    public String toString() {
        return "MiruActorId{" + "actorId=" + actorId + '}';
    }
}
