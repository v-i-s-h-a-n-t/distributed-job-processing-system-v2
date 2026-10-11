package com.cs324a2.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import org.junit.jupiter.api.Test;

/**
 * Headless-safe checks for the GUI's spinner plumbing: typed-but-uncommitted
 * editor text must be flushed via {@code commitEdit()}, values are read as
 * {@code long} (never an {@code (Integer)} cast), ports are bounds-checked
 * before narrowing to {@code int}, and 64-bit bounds are never silently
 * corrupted by a double round-trip.
 */
class ClientSpinnerTest {

    private static JSpinner longSpinner(long value) {
        JSpinner spinner = new JSpinner(
                new LongSpinnerModel(value, Long.MIN_VALUE, Long.MAX_VALUE, 1L));
        spinner.setEditor(new JSpinner.NumberEditor(spinner, "0"));
        return spinner;
    }

    private static void type(JSpinner spinner, String text) {
        ((JSpinner.DefaultEditor) spinner.getEditor()).getTextField().setText(text);
    }

    @Test
    void commitAndReadLongFlushesUncommittedEditorText() {
        JSpinner spinner = new JSpinner(
                new SpinnerNumberModel(1L, Long.MIN_VALUE, Long.MAX_VALUE, 1L));
        JSpinner.NumberEditor editor = new JSpinner.NumberEditor(spinner, "0");
        spinner.setEditor(editor);
        editor.getTextField().setText("4242");
        // No Enter pressed: getValue() still holds the old model value ...
        assertEquals(1L, ((Number) spinner.getValue()).longValue());
        // ... but the helper commits the pending edit first.
        assertEquals(4242L, ClientApp.commitAndReadLong(spinner, "start"));
    }

    @Test
    void commitAndReadLongRejectsNonNumericText() {
        JSpinner spinner = new JSpinner(
                new SpinnerNumberModel(1L, Long.MIN_VALUE, Long.MAX_VALUE, 1L));
        JSpinner.NumberEditor editor = new JSpinner.NumberEditor(spinner, "0");
        spinner.setEditor(editor);
        editor.getTextField().setText("not-a-number");
        assertThrows(IllegalArgumentException.class,
                () -> ClientApp.commitAndReadLong(spinner, "start"));
    }

    @Test
    void commitAndReadLongRejectsEmptyText() {
        JSpinner spinner = longSpinner(1L);
        type(spinner, "   ");
        assertThrows(IllegalArgumentException.class,
                () -> ClientApp.commitAndReadLong(spinner, "start"));
    }

    @Test
    void largeRangeBoundsRoundTripExactly() {
        // A double-backed spinner silently corrupts all of these.
        long[] exact = {
            100_000_000L,
            100_000_000_000_000L,
            2_500_000_000_000_000L,
            9_007_199_254_740_993L,   // 2^53 + 1, the double rounding boundary
            Long.MAX_VALUE,
            -5L,
        };
        for (long candidate : exact) {
            JSpinner spinner = longSpinner(1L);
            type(spinner, String.valueOf(candidate));
            assertEquals(candidate, ClientApp.commitAndReadLong(spinner, "end"),
                    "value must survive the spinner exactly: " + candidate);
            assertEquals(String.valueOf(candidate),
                    ClientApp.editorText(spinner),
                    "the field must still show the typed digits: " + candidate);
        }
    }

    @Test
    void rejectsTrailingGarbageThatLenientParserWouldAccept() {
        // NumberEditor parses "12x" as 12; that must not silently shrink a bound.
        JSpinner spinner = longSpinner(1L);
        type(spinner, "12x");
        assertThrows(IllegalArgumentException.class,
                () -> ClientApp.commitAndReadLong(spinner, "end"));
    }

    @Test
    void rejectsInputBeyond64BitsInsteadOfClampingToMaxValue() {
        // 23 digits used to clamp silently to Long.MAX_VALUE.
        JSpinner spinner = longSpinner(1L);
        type(spinner, "99999999999999999999999");
        assertThrows(IllegalArgumentException.class,
                () -> ClientApp.commitAndReadLong(spinner, "end"));
    }

    @Test
    void longValuedPortIsBoundsCheckedInsteadOfClassCast() {
        // commitEdit() on the Integer-based port model can leave a Long behind
        // (e.g. after typing a huge number); an (Integer) cast would throw
        // ClassCastException instead of a validation error.
        JSpinner port = new JSpinner(new SpinnerNumberModel(1099, 1, 65535, 1));
        port.setValue(70_000L);
        assertThrows(IllegalArgumentException.class,
                () -> ClientApp.toPort(ClientApp.commitAndReadLong(port, "port")));
    }

    @Test
    void toPortAcceptsBoundariesAndRejectsOutOfRange() {
        assertEquals(1, ClientApp.toPort(1L));
        assertEquals(65_535, ClientApp.toPort(65_535L));
        assertThrows(IllegalArgumentException.class, () -> ClientApp.toPort(0L));
        assertThrows(IllegalArgumentException.class, () -> ClientApp.toPort(65_536L));
        assertThrows(IllegalArgumentException.class, () -> ClientApp.toPort(-1L));
        assertThrows(IllegalArgumentException.class, () -> ClientApp.toPort(99_999_999_999L));
    }

    @Test
    void longSpinnerModelStepsAndClampsIn64BitSpace() {
        LongSpinnerModel model = new LongSpinnerModel(5L, 0L, 10L, 2L);
        assertEquals(5L, model.getValue());
        assertEquals(7L, model.getNextValue());
        assertEquals(3L, model.getPreviousValue());
        model.setValue(10L);
        assertEquals(10L, model.getValue());
        assertEquals(null, model.getNextValue(), "stepping past max yields null");
        model.setValue(-5L);
        assertEquals(0L, model.getValue(), "setValue clamps to min");
        model.setValue(Long.MAX_VALUE);
        assertEquals(10L, model.getValue());
        assertThrows(IllegalArgumentException.class, () -> model.setValue("nope"));
    }
}
