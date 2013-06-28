package pl.edu.icm.oozierunner.toolbox;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import pl.edu.icm.oozierunner.OozieRunnerConstants;
import pl.edu.icm.oozierunner.OozieRunnerException;

public class HDFSHelper {

    FileSystem hdfs;

    public HDFSHelper(String hdfsUri, String hdfsUserName) {

        URI hdfsURI;

        if (hdfsUserName != null) {
            System.setProperty("HADOOP_USER_NAME", hdfsUserName);
        }

        if (!hdfsUri.startsWith("hdfs://") && !hdfsUri.startsWith("webhdfs://")) {
            hdfsUri = "hdfs://" + hdfsUri;
        }
        try {
            hdfsURI = new URI(hdfsUri);
        } catch (URISyntaxException e) {
            throw new OozieRunnerException("HDFS URI cannot be parsed.", e);
        }

        Configuration hdfsFSconf = new Configuration();
        hdfsFSconf.set("fs.hdfs.impl",
                "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            hdfs = FileSystem.get(hdfsURI, hdfsFSconf);
        } catch (IOException e) {
            throw new OozieRunnerException("HDFS FileSystem with URI "
                    + hdfsURI.toString() + " failed to be created.", e);
        }

    }

    public void copyToHDFS(String localDir, String hdfsDir) throws IOException {

        String localAbsoluteDir = new File(localDir).getAbsolutePath();
        Path localPath = new Path(localAbsoluteDir);
        Path hdfsPath = new Path(hdfsDir);

        hdfs.copyFromLocalFile(localPath, hdfsPath);
    }

    public void getOutputFiles(String hdfsDir, String localDir) throws IOException {

        String localAbsoluteDir = new File(localDir).getAbsolutePath();
        Path localPath = new Path(localAbsoluteDir);
        Path hdfsPath = new Path(hdfsDir);

        hdfs.copyToLocalFile(hdfsPath, localPath);
    }
}
