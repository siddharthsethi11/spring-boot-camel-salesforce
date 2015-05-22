package com.hrboss.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Model as the wrapper for the data set. All incoming data will be stored as {@code BasicDBObject}.
 *
 * @author kwidjaja
 * @see BasicDBObject
 * @since 25/04/14.
 */
public class DataSet extends BaseModel {

    /**
     * This enum is used to identify what is the source the data coming from.
     *
     * @author kwidjaja
     */
    public static enum DATA_SOURCE_TYPE {
        /**
         * Indicates that the data is coming from a file
         */
        FILE,

        /**
         * Indicates that the data is coming from a streaming connection, such as HTTP, external system, etc.
         */
        STREAMING
    }

    public static final String[] EXCLUDE_TIMESTAMP = new String[] { "insertAt", "updateAt", "password" };

    public static final String[] EXCLUDE_ALL = new String[] { "fields", "virtualFields", "calculatedFields", "rollupFields",
            "uniqueFieldMap", "orgSettingFieldMap", "positionOrgKeyFieldMap", "employeeOrgKeyFieldMap", "additionalFieldMap" };

    private static final long serialVersionUID = -3932250835858516024L;

    public DataSet() {
    }

    public DataSet(int size) {
        super(size);
    }

    public DataSet(String key, Object value) {
        super(key, value);
    }

    public DataSet(Map<String, Object> map) {
        super(map);
    }

    public String getName() {
        return containsField("name") ? getString("name") : "";
    }

    public void setName(String name) {
        put("name", name);
    }

    public int getSeqId() {
        if (containsField("seqId")) {
            return getInt("seqId");
        }

        return -1;
    }

    public void setSeqId(int seqId) {
        put("seqId", seqId);
    }

    public String getOwnerId() {
        return containsField("owner") ? getString("owner") : "";
    }

    public void setOwnerId(String ownerId) {
        put("owner", ownerId);
    }

    public String getDataType() {
        return containsField("type") ? getString("type") : "";
    }

    public void setDataType(String dataType) {
        put("type", dataType);
    }

    @SuppressWarnings("unused")
    public String getContainerId() {
        return containsField("containerId") ? getString("containerId") : "";
    }

    @SuppressWarnings("unused")
    public void setContainerId(String containerId) {
        put("containerId", containerId);
    }

    @SuppressWarnings("unused")
    public String getDataSourceId() {
        return containsField("dataSourceId") ? getString("dataSourceId") : "";
    }

    @SuppressWarnings("unused")
    public void setDataSourceId(String dataSourceId) {
        put("dataSourceId", dataSourceId);
    }

    public String getWorkspaceId() {
        return containsField("wsId") ? getString("wsId") : "";
    }

    public void setWorkspaceId(String workspaceId) {
        put("wsId", workspaceId);
    }

    public String getCategoryCode() {
        return containsField("category") ? getString("category") : "";
    }

    public void setCategoryCode(String categoryCode) {
        put("category", categoryCode);
    }

    public String getDataCollectionName() {
        return containsField("collection") ? getString("collection") : "";
    }

    public void setDataCollectionName(String dataCollectionName) {
        put("collection", dataCollectionName);
    }

    public boolean isValidated() {
        return getBoolean("validated");
    }

    public void setValidated(boolean isValidated) {
        put("validated", isValidated);
    }

    public boolean isAssociated() {
        return getBoolean("associated");
    }

    public void setAssociated(boolean isAssociated) {
        put("associated", isAssociated);
    }

    public boolean isTemporary() {
        return getBoolean("temporary");
    }

    public void setTemporary(boolean temporary) {
        put("temporary", temporary);
    }

    @SuppressWarnings("unused")
    public boolean isAllowMultipleKey() {
        return getBoolean("multiple");
    }

    public void setAllowMultipleKey(boolean multiple) {
        put("multiple", multiple);
    }

    @SuppressWarnings("unused")
    public boolean hasError() {
        return getBoolean("hasError");
    }

    public void setError(boolean hasError) {
        put("hasError", hasError);
    }

    public String getSourceUri() {
        return containsField("sourceUri") ? getString("sourceUri") : "";
    }

    public void setSourceUri(String sourceUri) {
        put("sourceUri", sourceUri);
    }

    public String getType() {
        return containsField("type") ? getString("type") : "";
    }

    public void setType(String type) {
        put("type", type);
    }

    public String getPassword() {
        return containsField("password") ? getString("password") : "";
    }

    public void setPassword(String password) {
        put("password", password);
    }

    public Boolean isLocked() {
        return containsField("isLocked") ? getBoolean("isLocked") : Boolean.FALSE;
    }

    public void setLocked(Boolean locked) {
        put("isLocked", locked);
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getOriginalFields() {
        if (containsField("fields")) {
            Object originalFields = get("fields");
            if (Map.class.isAssignableFrom(originalFields.getClass())) {
                return new BasicDBObject((Map) originalFields);
            }
        }

        return new BasicDBObject();
    }

    public void setOriginalFields(BasicDBObject originalFields) {
        if (originalFields != null) {
            put("fields", originalFields);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getVirtualFields() {
        if (containsField("virtualFields")) {
            Object virtualFields = get("virtualFields");
            if (Map.class.isAssignableFrom(virtualFields.getClass())) {
                return new BasicDBObject((Map) virtualFields);
            }
        }

        return new BasicDBObject();
    }

    public void setVirtualFields(BasicDBObject virtualFields) {
        if (virtualFields != null) {
            put("virtualFields", virtualFields);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getCalculatedFields() {
        if (containsField("calculatedFields")) {
            Object calculatedFields = get("calculatedFields");
            if (Map.class.isAssignableFrom(calculatedFields.getClass())) {
                return new BasicDBObject((Map) calculatedFields);
            }
        }

        return new BasicDBObject();
    }

    public void setCalculatedFields(BasicDBObject calculatedFields) {
        if (calculatedFields != null) {
            put("calculatedFields", calculatedFields);
        }
    }

    public BasicDBObject getRollupFields() {
        if (containsField("rollupFields")) {
            Object rollupFields = get("rollupFields");
            if (Map.class.isAssignableFrom(rollupFields.getClass())) {
                return new BasicDBObject((Map) rollupFields);
            }
        }

        return new BasicDBObject();
    }

    public void setRollupFields(BasicDBObject rollupFields) {
        if (rollupFields != null) {
            put("rollupFields", rollupFields);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getUniqueFieldMap() {
        if (containsField("uniqueFieldMap")) {
            Object fieldMap = get("uniqueFieldMap");
            if (Map.class.isAssignableFrom(fieldMap.getClass())) {
                return new BasicDBObject((Map) fieldMap);
            }
        }

        return new BasicDBObject();
    }

    public void setUniqueFieldMap(BasicDBObject fieldMap) {
        if (fieldMap != null) {
            put("uniqueFieldMap", fieldMap);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getOrgSettingFieldMap() {
        if (containsField("orgSettingFieldMap")) {
            Object fieldMap = get("orgSettingFieldMap");
            if (Map.class.isAssignableFrom(fieldMap.getClass())) {
                return new BasicDBObject((Map) fieldMap);
            }
        }

        return new BasicDBObject();
    }

    public void setOrgSettingFieldMap(BasicDBObject fieldMap) {
        if (fieldMap != null) {
            put("orgSettingFieldMap", fieldMap);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getPositionOrgKeyFieldMap() {
        if (containsField("positionOrgKeyFieldMap")) {
            Object fieldMap = get("positionOrgKeyFieldMap");
            if (Map.class.isAssignableFrom(fieldMap.getClass())) {
                return new BasicDBObject((Map) fieldMap);
            }
        }

        return new BasicDBObject();
    }

    public void setPositionOrgKeyFieldMap(BasicDBObject fieldMap) {
        if (fieldMap != null) {
            put("positionOrgKeyFieldMap", fieldMap);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getEmployeeOrgKeyFieldMap() {
        if (containsField("employeeOrgKeyFieldMap")) {
            Object fieldMap = get("employeeOrgKeyFieldMap");
            if (Map.class.isAssignableFrom(fieldMap.getClass())) {
                return new BasicDBObject((Map) fieldMap);
            }
        }

        return new BasicDBObject();
    }

    public void setEmployeeOrgKeyFieldMap(BasicDBObject fieldMap) {
        if (fieldMap != null) {
            put("employeeOrgKeyFieldMap", fieldMap);
        }
    }

    @SuppressWarnings("unchecked")
    public BasicDBObject getAdditionalFieldMap() {
        if (containsField("additionalFieldMap")) {
            Object fieldMap = get("additionalFieldMap");
            if (Map.class.isAssignableFrom(fieldMap.getClass())) {
                return new BasicDBObject((Map) fieldMap);
            }
        }

        return new BasicDBObject();
    }

    public void setAdditionalFieldMap(BasicDBObject fieldMap) {
        if (fieldMap != null) {
            put("additionalFieldMap", fieldMap);
        }
    }

    // rowCount
    public int getRowCount() {
        return containsField("rowCount") ? getInt("rowCount") : 0;
    }

    public void setRowCount(int rowCount) {
        put("rowCount", rowCount);
    }

    // primaryKeys
    @SuppressWarnings("unchecked")
    public List<String> getPrimaryKeys() {
        if (containsField("primaryKeys")) {
            Object primaryKeys = get("primaryKeys");
            if (List.class.isAssignableFrom(primaryKeys.getClass())) {
                return (List<String>) primaryKeys;
            }
        }

        return new ArrayList<>();
    }

    public void setPrimaryKeys(BasicDBList primaryKeys) {
        put("primaryKeys", primaryKeys);
    }

    // hasPK
    public boolean getHasPrimaryKey() {
        return containsField("hasPK") && getBoolean("hasPK");
    }

    public void setHasPrimaryKey(boolean hasPK) {
        put("hasPK", hasPK);
    }

    // foreignKeys
    @SuppressWarnings("rawtypes")
    public BasicDBObject getForeignKeys() {
        if (containsField("foreignKeys")) {
            Object foreignKeys = get("foreignKeys");
            if (Map.class.isAssignableFrom(foreignKeys.getClass())) {
                return new BasicDBObject((Map) foreignKeys);
            }
        }

        return new BasicDBObject();
    }

    public void setForeignKeys(BasicDBObject foreignKeys) {
        if (foreignKeys != null) {
            put("foreignKeys", foreignKeys);
        }
    }

    // hasFK
    public boolean getHasForeignKey() {
        return containsField("hasFK") && getBoolean("hasFK");
    }

    public void setHasForeignKey(boolean hasFK) {
        put("hasFK", hasFK);
    }

    // sampleBasic
    public String getSampleBasic() {
        return containsField("sampleBasic") ? getString("sampleBasic") : "";
    }

    public void setSampleBasic(String sampleBasic) {
        put("sampleBasic", sampleBasic);
    }

    // sampleValue
    public int getSampleValue() {
        return containsField("sampleValue") ? getInt("sampleValue") : 0;
    }

    public void setSampleValue(int sampleValue) {
        put("sampleValue", sampleValue);
    }

    // importJobId
    public String getImportJobId() {
        return containsField("importJobId") ? getString("importJobId") : "";
    }

    public void setImportJobId(String importJobId) {
        put("importJobId", importJobId);
    }

    // originalDataSetId
    public String getOriginalDataSetId() {
        return containsField("originalDataSetId") ? getString("originalDataSetId") : "";
    }

    public void setOriginalDataSetId(String originalDataSetId) {
        put("originalDataSetId", originalDataSetId);
    }

    // hasImport
    public boolean getHasImport() {
        return containsField("hasImport") && getBoolean("hasImport");
    }

    public void setHasImport(boolean hasImport) {
        put("hasImport", hasImport);
    }

    // status
    public String getStatus() {
        return containsField("status") ? getString("status") : "";
    }

    public void setStatus(String status) {
        put("status", status);
    }

    // version
    public int getVersion() {
        return containsField("version") ? getInt("version") : 0;
    }

    public void setVersion(int version) {
        put("version", version);
    }

    // firstImportedDate
    public Date getFirstImportedDate() {
        if (containsField("firstImportedDate")) {
            return getDate("firstImportedDate");
        }
        return null;
    }

    public void setFirstImportedDate(Date firstImportedDate) {
        put("firstImportedDate", firstImportedDate);
    }

    // roles
    @SuppressWarnings("unchecked")
    public List<String> getRoles() {
        if (containsField("roles")) {
            Object roles = get("roles");
            if (List.class.isAssignableFrom(roles.getClass())) {
                return (List<String>) roles;
            }
        }
        return new ArrayList<>();
    }

    public void setRoles(List<String> roles) {
        put("roles", roles);
    }
}
