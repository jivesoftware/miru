package com.jivesoftware.os.miru.service.schema;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVoidByte;
import com.jivesoftware.os.rcvs.inmemory.RowColumnValueStoreImpl;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RegistrySchemaProviderTest {

    @Test
    public void testSchemaProvider() throws Exception {
        MiruTenantId tenantId1 = new MiruTenantId("tenant1".getBytes());
        MiruSchema schema1 = new MiruSchema(new MiruFieldDefinition(0, "a"), new MiruFieldDefinition(1, "b"));
        MiruTenantId tenantId2 = new MiruTenantId("tenant2".getBytes());
        MiruSchema schema2 = new MiruSchema(new MiruFieldDefinition(0, "c"), new MiruFieldDefinition(1, "d"));

        RowColumnValueStoreImpl<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema> schemaRegistry = new RowColumnValueStoreImpl<>();
        RegistrySchemaProvider schemaProvider = new RegistrySchemaProvider(schemaRegistry, 10);

        schemaProvider.register(tenantId1, schema1, 1L);
        schemaProvider.register(tenantId2, schema2, 2L);

        assertEquals(schemaProvider.getSchema(tenantId1).fieldCount(), 2);
        assertEquals(schemaProvider.getSchema(tenantId2).fieldCount(), 2);

        assertEquals(schemaProvider.getSchema(tenantId1).getFieldDefinition(0).name, "a");
        assertEquals(schemaProvider.getSchema(tenantId1).getFieldDefinition(1).name, "b");
        assertEquals(schemaProvider.getSchema(tenantId2).getFieldDefinition(0).name, "c");
        assertEquals(schemaProvider.getSchema(tenantId2).getFieldDefinition(1).name, "d");
    }
}
