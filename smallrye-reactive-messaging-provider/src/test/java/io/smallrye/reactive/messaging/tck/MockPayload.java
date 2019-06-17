package io.smallrye.reactive.messaging.tck;

import java.util.Objects;

public class MockPayload {
    private String field1;
    private int field2;

    public MockPayload(String field1, int field2) {
        this.field1 = field1;
        this.field2 = field2;
    }

    public MockPayload() {
    }

    public String getField1() {
        return field1;
    }

    public int getField2() {
        return field2;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public void setField2(int field2) {
        this.field2 = field2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MockPayload that = (MockPayload) o;
        return field2 == that.field2 &&
                Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {

        return Objects.hash(field1, field2);
    }

    @Override
    public String toString() {
        return "MockPayload{" +
                "field1='" + field1 + '\'' +
                ", field2=" + field2 +
                '}';
    }
}
