package com.tripleone.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Func:
 * Created by tripleone on 18/3/7.
 */
public class HdfsTest {
    //查看hadoop的根目录下的hello文件
    public static final String HDFS_PATH = "hdfs://localhost:8020/testHdfsDir";

    public static final String DIR_PATH = "/testHdfsDir";//testHdfsDir";
    public static final String FILE_PATH_UPLOAD = "/testHdfsDir/file1";
    public static final String FILE_PATH_DOWNLOAD = "/testHdfsDir/file1";
    public static final String FILE_LOCAL_PATH_UPLOAD = "/Users/fengweijiao/Desktop/hadoop/testfile.txt";
    public static final String FILE_LOCAL_PATH_DOWNLOAD = "/Users/fengweijiao/Desktop/hadoop/testdown.txt";
    private static FsPermission permission;

    public static final String FWJ_PATH = "/user/fwj";//testHdfsDir";



    //列出文件
    public static void listFiles(String dirPath){
        Configuration conf = new Configuration();
        URI url = null;

        try {
            url = new URI(HDFS_PATH);
            FileSystem fs = FileSystem.get(url, conf);
            FileStatus[] files = fs.listStatus(new Path(dirPath));
            for (FileStatus file : files){
                System.out.println(file.getPath().toString());
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration();
            final FileSystem fs = FileSystem.get(new URI(HDFS_PATH),
                    conf);


            //创建文件夹
            fs.mkdirs(new Path(DIR_PATH));
            fs.mkdirs(new Path(DIR_PATH), permission);

            //上传文件
            final FSDataOutputStream out = fs.create(new Path(FILE_PATH_UPLOAD));
            final FileInputStream in = new FileInputStream(FILE_LOCAL_PATH_UPLOAD);
            IOUtils.copyBytes(in, out,1024, true);

            //下载文件
            final FSDataInputStream fsin = fs.open(new Path(FILE_PATH_DOWNLOAD));
            IOUtils.copyBytes(fsin, System.out, 1024, true);

            //删除文件夹
            fs.delete(new Path(DIR_PATH), true); //是否递归删除


//          查看是否文件
            boolean isFile = fs.isFile(new Path(FILE_PATH_DOWNLOAD));
            System.out.println(FILE_PATH_DOWNLOAD+" is file?"+isFile);

//           移动本地文件到hdfs
            fs.moveFromLocalFile(new Path(FILE_LOCAL_PATH_UPLOAD),
                    new Path(FILE_PATH_UPLOAD)
            );
//          复制本地文件到hdfs
            fs.copyFromLocalFile(new Path(FILE_LOCAL_PATH_UPLOAD),
                    new Path(FILE_PATH_UPLOAD));

            listFiles(FWJ_PATH);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }


}
