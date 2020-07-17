package com.ifchange.flink.sql.test;

import java.io.Serializable;

public class Sensor implements Serializable {

    private String id;

    private long time;

    private double temparature;


    public Sensor() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getTemparature() {
        return temparature;
    }

    public void setTemparature(double temparature) {
        this.temparature = temparature;
    }
}
