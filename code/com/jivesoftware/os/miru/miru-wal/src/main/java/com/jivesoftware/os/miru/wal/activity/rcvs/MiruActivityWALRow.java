package com.jivesoftware.os.miru.wal.activity.rcvs;

public class MiruActivityWALRow implements Comparable<MiruActivityWALRow> {
    private final int partitionId;

    public MiruActivityWALRow(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruActivityWALRow that = (MiruActivityWALRow) o;

        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return partitionId;
    }

    @Override
    public int compareTo(MiruActivityWALRow miruActivityWALRow) {
        return Integer.compare(partitionId, miruActivityWALRow.partitionId);
    }
}
