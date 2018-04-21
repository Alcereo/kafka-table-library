package com.github.alcereo.kafkatool;

public interface KTStorage<K,V> {
    void upsert(K key, V value);
}
