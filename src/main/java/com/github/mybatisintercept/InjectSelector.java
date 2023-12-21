package com.github.mybatisintercept;

import com.github.mybatisintercept.util.PlatformDependentUtil;

import java.util.LinkedList;
import java.util.concurrent.Callable;

public class InjectSelector implements AutoCloseable {
    private final Integer[] old = InjectConditionSQLInterceptor.getSelector();
    private final LinkedList<Integer[]> currentList = new LinkedList<>();

    public Integer[] begin(Integer... index) {
        Integer[] old = currentList.isEmpty() ? this.old : currentList.get(0);
        this.currentList.addFirst(index);
        InjectConditionSQLInterceptor.setSelector(index);
        return old;
    }

    public Integer[] end() {
        Integer[] oldAccessUser;
        if (currentList.isEmpty()) {
            oldAccessUser = this.old;
        } else {
            currentList.removeFirst();
            oldAccessUser = currentList.isEmpty() ? this.old : currentList.get(0);
        }
        InjectConditionSQLInterceptor.setSelector(oldAccessUser);
        return oldAccessUser;
    }

    public <T> T runOn(Integer[] accessUser, Callable<T> callable) {
        try {
            begin(accessUser);
            return callable.call();
        } catch (Exception e) {
            PlatformDependentUtil.sneakyThrows(e);
            return null;
        } finally {
            end();
        }
    }

    public void runOn(Integer[] accessUser, Runnable runnable) {
        try {
            begin(accessUser);
            runnable.run();
        } catch (Throwable e) {
            PlatformDependentUtil.sneakyThrows(e);
        } finally {
            end();
        }
    }

    @Override
    public void close() {
        InjectConditionSQLInterceptor.setSelector(old);
    }

    @FunctionalInterface
    public interface Runnable {
        void run() throws Throwable;
    }

}