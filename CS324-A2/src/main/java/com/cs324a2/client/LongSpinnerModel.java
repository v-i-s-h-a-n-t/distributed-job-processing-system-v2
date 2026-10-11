package com.cs324a2.client;

import javax.swing.SpinnerNumberModel;

/**
 * Exactly 64-bit spinner model for PRIMESUM bounds and other large values.
 *
 * <p>{@link SpinnerNumberModel}'s public constructor only has a special case
 * for {@link Integer}; every other argument combination — including all
 * {@code Long}s — falls through to a {@link Double}-backed model. That silently
 * corrupts large range bounds: {@code 9007199254740993} (2^53+1) is stored as
 * {@code 9007199254740992}, {@code Long.MAX_VALUE} renders as
 * {@code 9223372036854776000}, and clamping compares {@code doubleValue()}s.
 * The GUI would then submit a different range than the user typed.
 *
 * <p>This subclass keeps an exact {@code long} and stays a
 * {@code SpinnerNumberModel} so {@link javax.swing.JSpinner.NumberEditor} (and
 * therefore {@code commitEdit()}) keeps working.
 */
public final class LongSpinnerModel extends SpinnerNumberModel {

    private final long min;
    private final long max;
    private final long step;
    private long exact;

    public LongSpinnerModel(long value, long min, long max, long step) {
        // Super is invoked purely so NumberEditor accepts this model; all
        // behaviour is overridden below, so its internal (Integer) state is
        // never consulted.
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