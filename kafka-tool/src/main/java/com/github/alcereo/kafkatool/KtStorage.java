package com.github.alcereo.kafkatool;

public interface KtStorage<K,V> {
    void upsert(K key, V value);
}
