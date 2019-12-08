
import java.util.Properties;
import java.util.List;
import java.util.LinkedList;
import java.util.stream.Collectors;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.SequenceInputStream;
import java.io.ByteArrayInputStream;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.security.MessageDigest;

import javax.xml.bind.DatatypeConverter;

public class MyDedup {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.err.println("Invalid arguments");
      System.exit(1);
    }

    // Read config file
    Properties conf = new Properties();
    conf.load(new FileInputStream("cloud/config.properties"));

    // Select storage backend
    Store store = null;
    switch(args[args.length-1]) {
      case "local":
        store = new Store.LocalStore(conf);
        break;
      case "s3":
        store = new Store.S3Store(conf);
        break;
      case "azure":
        store = new Store.AzureStore(conf);
        break;
      default:
        System.err.println("Invalid store: "+ args[args.length-1]);
        System.exit(1);
    }

    // Read index
    Index index = new Index(store);

    // Select operation
    switch(args[0]) {
      case "upload":
        if(args.length < 7) {
          System.err.println("Invalid arguments");
          System.exit(1);
        }
        upload(
            index, args[5],
            Integer.parseInt(args[1]),
            Integer.parseInt(args[2]),
            Integer.parseInt(args[3]),
            Integer.parseInt(args[4]));
        break;
      case "download":
        download(index, args[1]);
        break;
      case "delete":
        delete(index, args[1]);
        break;
      default:
        System.err.println("Invalid operation: "+ args[0]);
        System.exit(1);
    }

    // Write index
    index.persist();
  }

  /* ===========================================================
   * Variable Size Chunking
   */
  static class Chunk {
    public byte[] buf;
    public int offset = 0;
    public int len = 0;
    public String hash = null;
    public Chunk(byte[] t, int m) {buf = t; len = m;}
  }
  static Chunk nextChunk(List<Chunk> chunks, Chunk currentChunk, int m) {
    chunks.add(currentChunk);
    Chunk nextChunk = new Chunk(currentChunk.buf, m);
    nextChunk.offset = currentChunk.offset + currentChunk.len;
    return nextChunk;
  }
  static int modPow(int b, int e, int m) {
    int c = 1;
    for(int i=0; i<e; i++) {
      c = (c*b) & (m-1);
    }
    return c;
  }
  static List<Chunk> findChunks(byte[] t, int m, int q, int d, int maxChunkSize) {
    LinkedList<Chunk> chunks = new LinkedList<Chunk>();
    
    // window > file, return 1 chunk
    if(m >= t.length) {
      chunks.add(new Chunk(t, t.length));
      return chunks;
    }

    // reuse d^(m-1) mod q
    int dm1q = modPow(d, m-1, q);

    int rfp = 0;
    Chunk c = new Chunk(t, m);
    while(true) {
      // last chunk
      if(c.offset + c.len >= t.length) {
        c.len = t.length - c.offset;
        chunks.add(c);
        break;
      }
      // chunk size cut-off
      if(c.len >= maxChunkSize) {
        c.len = maxChunkSize;
        c = nextChunk(chunks, c, m);
        continue;
      }
      // compute RFP
      // given that q is a power of 2 (q = 2^n)
      // a mod q ==  a & (q-1), where & is bitwise-AND
      if(c.offset == 0 && c.len == m) {
        rfp = 0;
        for(int i = 1; i <= m; i++) {
          rfp += ((t[i-1] & (q-1)) * modPow(d, m-i, q)) & (q-1);
          rfp &= (q-1);
        }
      } else {
        int ibmq = t[c.offset + c.len - 1] & (q-1);
        int obmq = t[c.offset + c.len - m] & (q-1);
        rfp = (rfp - ((dm1q * obmq) & (q-1))) & (q-1);
        rfp = (rfp * (d & (q-1))) & (q-1);
        rfp = (rfp + ibmq) & (q-1);
      }
      // select interest RFP
      if(rfp == 0) {
        c = nextChunk(chunks, c, m);
        continue;
      }
      c.len++;
    }

    return chunks;
  }

  /* ===========================================================
   * Upload Operation
   */
  static void upload(Index index, String file, int m, int q, int maxChunkSize, int d)
    throws Exception {
    if(index.fileRecipe.containsKey(file)) {
      System.err.println("Error: file exists.");
      System.exit(1);
    }

    // read file & find chunks
    byte[] t = Files.readAllBytes(Paths.get(file));
    List<Chunk> chunks = findChunks(t, m, q, d, maxChunkSize);

    // hash & upload chunks in parallel
    chunks.parallelStream().forEach(c -> {
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.update(c.buf, c.offset, c.len);
        c.hash = DatatypeConverter.printHexBinary(md.digest());
        InputStream bais = new ByteArrayInputStream(c.buf, c.offset, c.len);
        index.uploadChunk(c.hash, bais);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    });

    // write file recipe
    List<String> chunkList =
      chunks.stream().map(c -> c.hash)
      .collect(Collectors.toList());
    index.fileRecipe.put(file, chunkList);

    // statistics reporting
    index.reportStatistics();
  }

  /* ===========================================================
   * Download Operation
   */
  static void download(Index index, String file) throws Exception {
    if(!index.fileRecipe.containsKey(file)) {
      System.err.println("Error: no such file.");
      System.exit(1);
    }
    // download chunks in parallel and concat
    InputStream fileData =
      index.fileRecipe.get(file)
      .parallelStream()
      .map(hash -> index.downloadChunk(hash))
      .reduce(
          new ByteArrayInputStream(new byte[0]),
          (acc, is) -> new SequenceInputStream(acc, is));
    Files.copy(fileData, Paths.get(file));
  }

  /* ===========================================================
   * Delete Operation
   */
  static void delete(Index index, String file) {
    if(!index.fileRecipe.containsKey(file)) {
      System.err.println("Error: no such file.");
      System.exit(1);
    }
    index.fileRecipe
      .get(file)
      .parallelStream()
      .forEach(hash -> index.deleteChunk(hash));
    index.fileRecipe.remove(file);
  }
}

