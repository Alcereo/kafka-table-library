package com.github.alcereo.kafkatool;

public interface KtConsumerLoop<K,V> extends AutoCloseable{

    void start();

}
