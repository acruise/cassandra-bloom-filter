package com.facebook.infrastructure.utils;

import java.util.Iterator;

public interface ResetableIterator<T> extends Iterator<T> {
    public void reset();

    int size();
}
