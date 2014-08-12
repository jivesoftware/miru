package com.jivesoftware.os.miru.service.index;

public interface BulkImport<T> {

    void bulkImport(BulkExport<T> importItems) throws Exception;

}
