package com.github.alcereo.kafkalibrary;

public interface KTStorage<K,V> {
    void upsert(K key, V value);
}
