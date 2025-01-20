package com.gotocompany.depot;

public interface SinkFactory {
    void init();
    Sink create();
}
