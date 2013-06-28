package pl.edu.icm.oozierunner;

public class OozieRunnerConstants {
    public static final String OOZIE_SERVICE_URI = "oozieServiceURI";

    public static final String HDFS_URI = "nameNode";
    public static final String HDFS_WORKING_DIR_URI = "hdfsWorkingDirURI";
    public static final String HDFS_USER_NAME = "hdfsUserName";
    public static final String SYSTEM_USER_NAME = "user.name";
    public static final String HDFS_DIR_INPUT_DATA = "hdfsDirInputData";
    public static final String HDFS_DIR_OUTPUT_DATA = "hdfsDirOutputData";
    public static final String LOCAL_DIR_INPUT_DATA = "localDirInputData";
    public static final String WORKFLOW_DIR = "wfDir";

    public static final String IT_ENV_PLACEHOLDER = "${IT.env}";
    public static final String IT_ENV_PROPERTIES_LOCATION = "configIT/env/IT-env-"
            + IT_ENV_PLACEHOLDER + ".properties";
    public static final String IT_WF_PROPERTIES_LOCATION = "configIT/wf/IT-wf.properties";

    public static final String PLACEHOLDER_PREFIX_NAME = "plcaeholder.prefix";
    public static final String PLACEHOLDER_SUFFIX_NAME = "placeholder.suffix";
    public static final String PLACEHOLDER_VALUE_SEPARATOR_NAME = "placeholder.valueSeparator";
    public static final String PLACEHOLDER_IGNORE_UNRESOLVABLE_PLACEHOLDERS_NAME = "placeholder.ignoreUnresolvablePlaceholders";
    public static final String PLACEHOLDER_PREFIX_DEFAULT = "${";
    public static final String PLACEHOLDER_SUFFIX_DEFAULT = "}";
    public static final String PLACEHOLDER_VALUE_SEPARATOR_DEFAULT = ":";
    public static final String PLACEHOLDER_IGNORE_UNRESOLVABLE_PLACEHOLDERS_DEFAULT = "false";
}
