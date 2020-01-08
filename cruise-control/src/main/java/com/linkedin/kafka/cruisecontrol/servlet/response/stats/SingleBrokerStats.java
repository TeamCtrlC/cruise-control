/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

public class SingleBrokerStats {
    protected static final String HOST = "host";
    protected static final String BROKER = "broker";

    private final String _host;
    private final int _id;
    private final boolean _isEstimated;

    SingleBrokerStats(String host, int id, boolean isEstimated) {
        _host = host;
        _id = id;
        _isEstimated = isEstimated;
    }

    public String host() {
        return _host;
    }

    public int id() {
        return _id;
    }

    public boolean isEstimated() {
        return _isEstimated;
    }
}