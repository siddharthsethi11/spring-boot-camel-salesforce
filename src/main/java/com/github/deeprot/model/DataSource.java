package com.github.deeprot.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;

/**
 * Model as the wrapper for the data source info model.
 *
 * @author Dhoan
 * @since 08/12/2014
 */
public class DataSource extends BaseModel {

    public static final String CSV_SEPARATOR_PARAM = "csvSeparator";
    public static final String NO_HEADER_PARAM = "noHeader";
    public static final String SHEET_INDEX_PARAM = "sheetIndex";
    public static final String SHEET_NAME_PARAM = "sheetName";
    public static final String ATTRIBUTE_PATH_PARAM = "attributePath";
    public static final String XML_TAG_PARAM = "xmlTag";

    /**
     * 
     */
    private static final long serialVersionUID = 1634974432432346689L;

    public DataSource() {

    }

    public DataSource(int size) {

        super(size);
    }

    public DataSource(String key, Object value) {

        super(key, value);
    }

    public DataSource(Map<String, Object> m) {

        super(m);
    }

    public String getUserId() {

        return containsField("userId") ? getString("userId") : "";
    }

    public void setUserId(String userId) {

        put("userId", userId);
    }

    public String getAlias() {

        return containsField("alias") ? getString("alias") : "";
    }

    public void setAlias(String alias) {

        put("alias", alias);
    }

    public String getDescription() {

        return containsField("description") ? getString("description") : "";
    }

    public void setDescription(String description) {

        put("description", description);
    }

    public String getName() {

        return containsField("name") ? getString("name") : "";
    }

    public void setName(String name) {

        put("name", name);
    }

    public String getType() {

        return containsField("type") ? getString("type") : "";
    }

    public void setType(String type) {

        put("type", type);
    }

    public String getContainerId() {

        return containsField("containerId") ? getString("containerId") : "";
    }

    public void setContainerId(String containerId) {

        put("containerId", containerId);
    }

    public boolean isHR() {

        return containsField("isHR") ? getBoolean("isHR") : false;
    }

    public void setHR(boolean isHR) {

        put("isHR", isHR);
    }

    public boolean isActive() {
        return getBoolean("active");
    }

    public void setActive(boolean active) {
        put("active", active);
    }

    public String getHost() {

        return containsField("host") ? getString("host") : "";
    }

    public void setHost(String host) {

        put("host", host);
    }

    // fileName
    public String getFileName() {

        return containsField("fileName") ? getString("fileName") : "";
    }

    public void setFileName(String fileName) {

        put("fileName", fileName);
    }

    // extension
    public String getExtension() {

        return containsField("extension") ? getString("extension") : "";
    }

    public void setExtension(String extension) {

        put("extension", extension);
    }

    // size
    public long getSize() {

        return containsField("size") ? getLong("size") : 0;
    }

    public void setSize(long size) {

        put("size", size);
    }

    // s3FileName
    public String getS3FileName() {

        return containsField("s3FileName") ? getString("s3FileName") : "";
    }

    public void setS3FileName(String s3FileName) {

        put("s3FileName", s3FileName);
    }

    // params
    @SuppressWarnings("rawtypes")
    public BasicDBObject getParameters() {

        if (containsField("params")) {
            Object params = get("params");

            if (Map.class.isAssignableFrom(params.getClass())) {
                return new BasicDBObject((Map) params);
            }
        }

        return new BasicDBObject();
    }

    public void setParameters(BasicDBObject params) {

        if (params == null) {
            params = new BasicDBObject();
        }

        put("params", params);
    }

    // lastModifiedDate
    public Date getLastModifiedDate() {
        if (containsField("lastModifiedDate")) {
            return getDate("lastModifiedDate");
        }

        return null;
    }

    public void setLastModifiedDate(Date lastModifiedDate) {
        put("lastModifiedDate", lastModifiedDate);
    }

    // version
    public int getVersion() {

        return containsField("version") ? getInt("version") : 0;
    }

    public void setVersion(int version) {

        put("version", version);
    }

    // port
    public String getPort() {

        return containsField("port") ? getString("port") : "";
    }

    public void setPort(String port) {

        put("port", port);
    }

    // DBName
    public String getDBName() {

        return containsField("DBName") ? getString("DBName") : "";
    }

    public void setDBName(String name) {

        put("DBName", name);
    }

    // DBUserName
    public String getDBUserName() {

        return containsField("DBUserName") ? getString("DBUserName") : "";
    }

    public void setDBUserName(String username) {

        put("DBUserName", username);
    }

    // DBPassword
    public String getDBPassword() {

        return containsField("DBPassword") ? getString("DBPassword") : "";
    }

    public void setDBPassword(String password) {

        put("DBPassword", password);
    }

    // DBType
    public String getDBType() {

        return containsField("DBType") ? getString("DBType") : "";
    }

    public void setDBType(String dbType) {

        put("DBType", dbType);
    }

    // DBSchema
    public String getDBSchema() {

        return containsField("DBSchema") ? getString("DBSchema") : "";
    }

    public void setDBSchema(String DBSchema) {

        put("DBSchema", DBSchema);
    }

    // roles
    @SuppressWarnings("unchecked")
    public List<String> getRoles() {
        if (containsField("roles")) {
            Object roles = get("roles");
            if (roles != null && List.class.isAssignableFrom(roles.getClass())) {
                return (List<String>) roles;
            }
        }
        return new ArrayList<>();
    }

    public void setRoles(List<String> roles) {
        put("roles", roles);
    }

    // owner
    public String getOwner() {
        return containsField("owner") ? getString("owner") : "";
    }

    public void setOwner(String ownerId) {
        put("owner", ownerId);
    }

    // email
    public String getEmail() {

        return containsField("email") ? getString("email") : "";
    }

    public void setEmail(String email) {

        put("email", email);
    }

}
