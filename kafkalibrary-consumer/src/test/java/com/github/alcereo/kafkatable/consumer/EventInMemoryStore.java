package com.github.alcereo.kafkatable.consumer;

import org.springframework.stereotype.Component;
import processing.DeviceEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

@Component
public class EventInMemoryStore {

    private BoundedDeq<DeviceEvent> events = new BoundedDeq<>(20);


    public synchronized void addEvent(DeviceEvent event){
        events.addFirst(event);
    }

    public List<DeviceEvent> getAll(){
        return new ArrayList<>(events);
    }


    static class BoundedDeq<T> extends ArrayDeque<T> {

        private final int bound;

        BoundedDeq(int bound) {
            this.bound = bound;
        }

        @Override
        public void addFirst(T t) {
            super.addFirst(t);

            if (size()>bound)
                removeLast();
        }
    }

}
