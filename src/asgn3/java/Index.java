
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Collection;
import java.util.stream.Collectors;

import java.io.File;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Index {
  private static final String indexFile = "mydedup.index";
  private static final String chunkPath = "chunks";

  // Chunk hash => Reference Count
  public HashMap<String,Integer> chunkCount;

  // Chunk hash => Reference Count
  public HashMap<String,Integer> chunkSizes;

  // File name => Chunk hash list
  public HashMap<String,List<String>> fileRecipe;

  private Store store;

  public Index(Store store) throws ClassNotFoundException {
    this.store = store;
    try {
      InputStream is = store.read(indexFile);
      if(is != null) {
        ObjectInputStream ois = new ObjectInputStream(is);
        chunkCount = (HashMap<String,Integer>) ois.readObject();
        chunkSizes = (HashMap<String,Integer>) ois.readObject();
        fileRecipe = (HashMap<String,List<String>>) ois.readObject();
        ois.close();
      } else {
        chunkCount = new HashMap<String,Integer>();
        chunkSizes = new HashMap<String,Integer>();
        fileRecipe = new HashMap<String,List<String>>();
      }
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void persist() {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(chunkCount);
      oos.writeObject(chunkSizes);
      oos.writeObject(fileRecipe);
      oos.close();
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      store.write(indexFile, bais);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void reportStatistics() {
    List<String> logicalChunks =
      fileRecipe.values().stream()
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
    long logicalSize =
      logicalChunks.stream()
      .map(hash -> (long)chunkSizes.get(hash))
      .mapToLong(Long::longValue).sum();

    Set<String> physicalChunks =
      chunkCount.keySet();
    long physicalSize =
      physicalChunks.stream()
      .map(hash -> (long)chunkSizes.get(hash))
      .mapToLong(Long::longValue).sum();
    
    System.out.println("Report Output:");
    System.out.println(
        "Total number of chunks in storage: "+
        logicalChunks.size());
    System.out.println(
        "Number of unique chunks in storage: "+
        physicalChunks.size());
    System.out.println(
        "Number of bytes in storage with deduplication: "+
        physicalSize);
    System.out.println(
        "Number of bytes in storage without deduplication: "+
        logicalSize);
    System.out.println(
        "Space saving: "+
        (1 - ((double)physicalSize) / logicalSize));
  }

  public void uploadChunk(String hash, InputStream chunk) {
    try {
      if(chunkCount.containsKey(hash)) {
        synchronized(this) {
          chunkCount.put(hash, chunkCount.get(hash) + 1);
        }
      } else {
        synchronized(this) {
          chunkCount.put(hash, 1);
          chunkSizes.put(hash, chunk.available());
        }
        store.write(new File(chunkPath, hash).toString(), chunk);
      }
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public InputStream downloadChunk(String hash) {
    return store.read(new File(chunkPath, hash).toString());
  }

  public void deleteChunk(String hash) {
    if(chunkCount.get(hash) > 1) {
      synchronized(this) {
        chunkCount.put(hash, chunkCount.get(hash) - 1);
      }
    } else {
      synchronized(this) {
        chunkCount.remove(hash);
        chunkSizes.remove(hash);
      }
      store.delete(new File(chunkPath, hash).toString());
    }
  }
}

