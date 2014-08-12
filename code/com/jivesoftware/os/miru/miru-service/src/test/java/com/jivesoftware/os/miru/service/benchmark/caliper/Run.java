package com.jivesoftware.os.miru.service.benchmark.caliper;

import com.google.common.base.Objects;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public final class Run {
    static final Run DEFAULT = new Run();

    private UUID id;
    private String label;
    private String startTime;

    private Run() {
        this.id = new UUID(0, 0);
        this.label = "";

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        this.startTime = format.format(new Date());
    }

    private Run(Builder builder) {
        this.id = builder.id;
        this.label = builder.label;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        this.startTime = format.format(builder.startTime);
    }

    public UUID id() {
        return id;
    }

    public String label() {
        return label;
    }

    public String startTime() {
        return startTime;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Run) {
            Run that = (Run) obj;
            return this.id.equals(that.id)
                && this.label.equals(that.label)
                && this.startTime.equals(that.startTime);
        } else {
            return false;
        }
    }

    @Override public int hashCode() {
        return Objects.hashCode(id, label, startTime);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
            .add("id", id)
            .add("label", label)
            .add("startTime", startTime)
            .toString();
    }

    public static final class Builder {
        private UUID id;
        private String label = "";
        private Date startTime;

        public Builder(UUID id) {
            this.id = checkNotNull(id);
        }

        public Builder label(String label) {
            this.label = checkNotNull(label);
            return this;
        }

        public Builder startTime(Date startTime) {
            this.startTime = checkNotNull(startTime);
            return this;
        }

        public Run build() {
            checkState(id != null);
            checkState(startTime != null);
            return new Run(this);
        }
    }
}