package pl.edu.icm.oozierunner;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import pl.edu.icm.oozierunner.toolbox.HDFSHelper;

public class OozieRunnerIO {

    protected URI hdfsURI;
    protected HDFSHelper hdfsHelper;
    protected String localDirInputData;
    protected String hdfsDirInputData;
    protected String hdfsDirOutputData;

    public OozieRunnerIO(Properties wfProperties, String userName) {

        //String hdfsUserName = wfProperties.getProperty(OozieRunnerConstants.HDFS_USER_NAME);
        //String hdfsURIName = wfProperties.getProperty(OozieRunnerConstants.HDFS_URI);


        localDirInputData = wfProperties.getProperty(OozieRunnerConstants.LOCAL_DIR_INPUT_DATA);
        hdfsDirInputData = wfProperties.getProperty(OozieRunnerConstants.HDFS_DIR_INPUT_DATA);
        hdfsDirOutputData = wfProperties.getProperty(OozieRunnerConstants.HDFS_DIR_OUTPUT_DATA);
        
        hdfsHelper = new HDFSHelper(hdfsDirInputData, userName);
    }

    public void copyInputFiles() throws IOException {
        if (localDirInputData != null && hdfsDirInputData != null) {
            hdfsHelper.copyToHDFS(localDirInputData, hdfsDirInputData);
        }
    }

    public File getOutputFiles() throws IOException {
        if (hdfsDirOutputData != null) {

            File outputDir = Files.createTempDir();
            outputDir.deleteOnExit();
            hdfsHelper.getOutputFiles(hdfsDirOutputData, outputDir.getPath());

            return outputDir;
        } else {
            return null;
        }
    }
}
