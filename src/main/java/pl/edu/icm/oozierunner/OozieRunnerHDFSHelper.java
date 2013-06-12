package pl.edu.icm.oozierunner;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OozieRunnerHDFSHelper {

    protected URI hdfsURI;
    protected String hdfsWorkingDirURI;
    protected String hdfsUserName;
    protected FileSystem hdfsFS;
    protected String localDirInputData;
    protected String hdfsDirInputData;
    protected String hdfsDirOutputData;

    public OozieRunnerHDFSHelper(Properties wfProperties) {

        hdfsUserName = wfProperties.getProperty(OozieRunnerConstants.HDFS_USER_NAME);
        if (hdfsUserName != null) {
            System.setProperty("HADOOP_USER_NAME", hdfsUserName);
        }

        String hdfsURIName = wfProperties.getProperty(OozieRunnerConstants.HDFS_URI);
        if (! hdfsURIName.startsWith("hdfs://")) {
            hdfsURIName = "hdfs://" + hdfsURIName;
        }
        try {
            hdfsURI = new URI(hdfsURIName);
        } catch (URISyntaxException e) {
            throw new OozieRunnerException("HDFS URI cannot be parsed.", e);
        }

        hdfsWorkingDirURI = wfProperties.getProperty(OozieRunnerConstants.HDFS_WORKING_DIR_URI);
        if (hdfsWorkingDirURI == null) {
            throw new OozieRunnerException("Property "
                    + OozieRunnerConstants.HDFS_WORKING_DIR_URI
                    + " cannot be empty.");
        }

        localDirInputData = wfProperties.getProperty(OozieRunnerConstants.LOCAL_DIR_INPUT_DATA);
        hdfsDirInputData = wfProperties.getProperty(OozieRunnerConstants.HDFS_DIR_INPUT_DATA);
        hdfsDirOutputData = wfProperties.getProperty(OozieRunnerConstants.HDFS_DIR_OUTPUT_DATA);

        Configuration hdfsFSconf = new Configuration();
        hdfsFSconf.set("fs.hdfs.impl",
                "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            hdfsFS = FileSystem.get(hdfsURI, hdfsFSconf);
        } catch (IOException e) {
            throw new OozieRunnerException("HDFS FileSystem with URI "
                    + hdfsURI.toString() + " failed to be created.", e);
        }

    }

    public void copyInputFiles() throws IOException {
        if (localDirInputData != null && hdfsDirInputData != null) {

            String localAbsolutePath = new File(localDirInputData).getAbsolutePath();
            Path localPath = new Path(localAbsolutePath);
            Path hdfsPath = new Path(hdfsDirInputData);

            hdfsFS.copyFromLocalFile(localPath, hdfsPath);

        }
    }

    public File getOutputFiles() throws IOException {
        if (hdfsDirOutputData != null) {

            File outputDir = Files.createTempDir();
            outputDir.deleteOnExit();

            Path hdfsPath = new Path(hdfsDirOutputData);
            Path localPath = new Path(outputDir.getAbsolutePath());

            hdfsFS.copyToLocalFile(hdfsPath, localPath);

            System.out.println("    output directory: " + outputDir.getAbsolutePath());
            return outputDir;

        } else {
            return null;
        }
    }
}
