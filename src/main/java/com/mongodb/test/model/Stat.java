package com.mongodb.test.model;

import java.time.LocalDateTime;

import lombok.Data;

@Data
public class Stat {
    private LocalDateTime startAt;
    private LocalDateTime endAt;
    private String operation;
    private int batchSize;
    private long duration;
    private double tps;

    public void setDuration(long duration) {
        if (duration > this.duration)
            this.duration = duration;
        this.tps = (double)batchSize / this.duration * 1000;
    }
}
