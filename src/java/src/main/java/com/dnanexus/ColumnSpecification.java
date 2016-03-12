// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

package com.dnanexus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * A single column in a GTable column schema.
 */
public class ColumnSpecification {
    /**
     * Returns a column specification with the given column name and column type.
     *
     * @param name column name
     * @param type column type
     *
     * @return column specification
     */
    public static ColumnSpecification getInstance(String name, ColumnType type) {
        return new ColumnSpecification(name, type);
    }

    @JsonProperty
    private String name;

    @JsonProperty
    private ColumnType type;

    private ColumnSpecification() {
        // No-arg constructor for JSON deserialization.
    }

    private ColumnSpecification(String name, ColumnType type) {
        this.name = Preconditions.checkNotNull(name, "column name may not be null");
        this.type = Preconditions.checkNotNull(type, "column type may not be null");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnSpecification other = (ColumnSpecification) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (type != other.type) return false;
        return true;
    }

    /**
     * Returns the column name.
     *
     * @return column name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the column type.
     *
     * @return column type
     */
    public ColumnType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }
}
