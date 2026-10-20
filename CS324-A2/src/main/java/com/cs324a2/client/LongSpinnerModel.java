package com.cs324a2.client;

import javax.swing.SpinnerNumberModel;

/**
 * Spinner model that holds an exact long. SpinnerNumberModel falls back to
 * Double for Long args, which loses precision on large values.
 */
public final class LongSpinnerModel extends SpinnerNumberModel {

    private final long min;
    private final long max;
    private final long step;
    private long exact;

    public LongSpinnerModel(long value, long min, long max, long step) {
        // dummy super so NumberEditor accepts this model
        super(0, Integer.MIN_VALUE, Integer.MAX_VALUE, 1);
        if (step <= 0) {
            throw new IllegalArgumentException("step must be positive");
        }
        this.min = min;
        this.max = max;
        this.step = step;
        if (min > max) {
            throw new IllegalArgumentException("min must not be greater than max");
        }
        this.exact = clamp(value);
    }

    @Override
    public Object getValue() {
        return exact;
    }

    @Override
    public void setValue(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("value must be a Number");
        }
        long next = clamp(number.longValue());
        if (next == exact) {
            return;
        }
        exact = next;
        fireStateChanged();
    }

    @Override
    public Object getNextValue() {
        long next = exact > max - step ? max : exact + step;
        return next == exact ? null : next;
    }

    @Override
    public Object getPreviousValue() {
        long previous = exact < min + step ? min : exact - step;
        return previous == exact ? null : previous;
    }

    @Override
    public Comparable<?> getMinimum() {
        return min;
    }

    @Override
    public Comparable<?> getMaximum() {
        return max;
    }

    @Override
    public Number getStepSize() {
        return step;
    }

    private long clamp(long value) {
        return Math.max(min, Math.min(max, value));
    }
}