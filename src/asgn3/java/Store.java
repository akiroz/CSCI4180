
import java.util.Properties;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;

import java.net.URI;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.StorageException;


interface Store {
  public InputStream read(String path);
  public void write(String path, InputStream data);
  public void delete(String path);

  /* ===========================================================
   * Local Filesystem Store Implementation
   */
  static class LocalStore implements Store {
    String dataDir;

    public LocalStore(Properties conf) {
      dataDir = conf.getProperty("local.data.dir");
    }

    public InputStream read(String path) {
      try {
        return new FileInputStream(new File(dataDir, path));
      } catch(FileNotFoundException e) {
        return null;
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void write(String path, InputStream data) {
      try {
        File f = new File(dataDir, path);
        f.getParentFile().mkdirs();
        if(f.exists()) f.delete();
        Files.copy(data, f.toPath());
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void delete(String path) {
      new File(dataDir, path).delete();
    }
  }

  /* ===========================================================
   * Amazon S3 Store Implementation
   */
  static class S3Store implements Store {
    AmazonS3 s3;
    String bucket;

    public S3Store(Properties conf) {
      s3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(
            conf.getProperty("aws.s3.region"))
        .withCredentials(
            new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(
                conf.getProperty("aws.access.key"),
                conf.getProperty("aws.access.secret"))))
        .build();
      bucket = conf.getProperty("aws.s3.bucket");
    }

    public InputStream read(String path) {
      try {
        return s3.getObject(bucket, path).getObjectContent();
      } catch(AmazonS3Exception e) {
        // file does not exists
        return null;
      }
    }

    public void write(String path, InputStream data) {
      try {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(data.available());
        s3.putObject(bucket, path, data, meta);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void delete(String path) {
      s3.deleteObject(bucket, path);
    }
  }

  /* ===========================================================
   * Microsoft Azure Store Implementation
   */
  static class AzureStore implements Store {
    CloudBlobContainer container;

    public AzureStore(Properties conf) {
      try {
        container =
          new CloudBlobClient(
              new StorageUri(
                new URI(
                  conf.getProperty("azure.blob.uri"))))
          .getContainerReference(
              conf.getProperty("azure.blob.container"));
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public InputStream read(String path) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        container.getBlockBlobReference(path).download(baos);
        baos.close();
        return new ByteArrayInputStream(baos.toByteArray());
      } catch(StorageException e) {
        // file does not exists
        return null;
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void write(String path, InputStream data) {
      try {
        container
          .getBlockBlobReference(path)
          .upload(data, data.available());
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void delete(String path) {
      try {
        container
          .getBlockBlobReference(path)
          .deleteIfExists();
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
