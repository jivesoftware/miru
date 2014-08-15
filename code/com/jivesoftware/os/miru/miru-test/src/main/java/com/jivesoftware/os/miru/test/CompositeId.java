package com.jivesoftware.os.miru.test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.TenantId;

import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;

/**
*
*/
public final class CompositeId {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final HashFunction hash = Hashing.murmur3_128();

    public static Id createOrdered(String name, TenantId tenantId, String... ids) {
        checkArgument(ids.length != 0, "You must specify at least one item to composite");
        final Hasher hasher = hash.newHasher();
        hasher.putString(name);
        hasher.putString(tenantId.toStringForm());
        for (String id : ids) {
            hasher.putString(id, UTF8);
        }
        return new Id(hasher.hash().asLong() & Long.MAX_VALUE);
    }
}
