package main.master;


import main.ScalableFileSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileCache {
// TODO Finish this file cache
    List<String> cacheLRU;
    Map<String, byte[]> cacheMapping;

    public FileCache() {
        cacheLRU = new ArrayList<>();
        cacheMapping = new HashMap<>();
    }

    public boolean hasChunk(String chunkName) {
        return cacheMapping.containsKey(chunkName);
    }

    public byte[] getChunk(String chunkName) {
        cacheLRU.remove(chunkName);
        cacheLRU.add(chunkName);
        return cacheMapping.get(chunkName);
    }

    public void addChunk(String chunkName, byte[] data) {
        cacheLRU.add(chunkName);
        cacheMapping.put(chunkName, data);
        if(cacheLRU.size() > ScalableFileSystem.CACHE_CHUNK_SIZE) {
            String toRemove = cacheLRU.remove(0);
            cacheMapping.remove(toRemove);
        }
    }


}

