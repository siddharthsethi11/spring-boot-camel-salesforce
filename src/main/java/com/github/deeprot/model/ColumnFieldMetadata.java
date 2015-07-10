/**
 * Copyright (c) 2013 HRBoss
 */
package com.github.deeprot.model;

import java.util.Map;

/**
 * @author Nguyen Nhat Quang
 *
 */
public class ColumnFieldMetadata extends BaseModel {

    /**
     * 
     */
    private static final long serialVersionUID = -7459721919339348686L;

    public ColumnFieldMetadata() {
    }

    public ColumnFieldMetadata(int size) {
        super(size);
    }

    public ColumnFieldMetadata(String key, Object value) {
        super(key, value);
    }

    public ColumnFieldMetadata(Map<String, Object> m) {
        super(m);
    }

    // alias
    public String getAlias() {

        return containsField("alias") ? getString("alias") : "";
    }

    public void setAlias(String alias) {

        put("alias", alias);
    }

    // pk
    public boolean getPK() {

        return containsField("pk") ? getBoolean("pk") : false;
    }

    public void setPK(boolean pk) {

        put("pk", pk);
    }

    // fk
    public boolean getFK() {

        return containsField("fk") ? getBoolean("fk") : false;
    }

    public void setFK(boolean fk) {

        put("fk", fk);
    }

    // type
    public String getType() {

        return containsField("type") ? getString("type") : null;
    }

    public void setType(String type) {

        put("type", type);
    }

    // sqlType
    public String getSqlType() {

        return containsField("sqlType") ? getString("sqlType") : null;
    }

    public void setSqlType(String sqlType) {

        put("sqlType", sqlType);
    }

    // format
    public String getFormat() {

        return containsField("format") ? getString("format") : "";
    }

    public void setFormat(String format) {

        put("format", format);
    }

    // formula
    public String getFormula() {

        return containsField("formula") ? getString("formula") : "";
    }

    public void setFormula(String formula) {

        put("formula", formula);
    }

    // columnType
    public String getColumnType() {

        return containsField("columnType") ? getString("columnType") : "";
    }

    public void setColumnType(String columnType) {

        put("columnType", columnType);
    }

    // includeSelf
    public Boolean getIncludeSelf() {

        return containsField("includeSelf") ? getBoolean("includeSelf") : null;
    }

    public void setIncludeSelf(Boolean includeSelf) {

        put("includeSelf", includeSelf);
    }
}
