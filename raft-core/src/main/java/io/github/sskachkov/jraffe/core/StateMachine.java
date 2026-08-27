package io.github.sskachkov.jraffe.core;

/** Applies a committed log command to a backing store. */
public interface StateMachine {
    byte [] apply(byte [] command);
}
