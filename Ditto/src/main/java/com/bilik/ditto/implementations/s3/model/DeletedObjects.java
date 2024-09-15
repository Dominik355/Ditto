package com.bilik.ditto.implementations.s3.model;

import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;

import java.util.Objects;

public class DeletedObjects {

    public final int deleted;
    public final int errors;

    public DeletedObjects(int deleted, int errors) {
        this.deleted = deleted;
        this.errors = errors;
    }

    public static DeletedObjects fromDeleteObjectsResponse(DeleteObjectsResponse response) {
        return new DeletedObjects(response.deleted().size(), response.errors().size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletedObjects that = (DeletedObjects) o;
        return deleted == that.deleted && errors == that.errors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleted, errors);
    }

    @Override
    public String toString() {
        return "DeleteObjectsResponse{" +
                "deleted=" + deleted +
                ", errors=" + errors +
                '}';
    }
}
