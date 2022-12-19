package main.master;

import java.util.ArrayList;
import java.util.List;

public class Chunk {
    private final List<Node> hostNodes;
    private final int size;
    private final String chunkName;

    public Chunk(String chunkName, int size) {
        this.size = size;
        this.chunkName = chunkName;
        hostNodes = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Chunk)) {
            return false;
        }
        Chunk c = (Chunk) o;
        return c.chunkName.equals(chunkName);
    }

    public void addNode(Node node) {
        hostNodes.add(node);
    }

    public List<Node> getNodes() {
        return hostNodes;
    }

    public void removeNode(Node node) {
        hostNodes.remove(node);
    }

    public int getSize() {
        return size;
    }

    public String getChunkName() {
        return chunkName;
    }

}
