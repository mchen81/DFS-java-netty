package edu.usfca.cs.dfs.server;

import edu.usfca.cs.dfs.node.FileSystemTree;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.utils.BloomFilter;
import edu.usfca.cs.dfs.utils.ChannelHandlerContextUtils;
import edu.usfca.cs.dfs.utils.Constant;
import edu.usfca.cs.dfs.utils.FileProcessUtil;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;

public class NodeManager {

    static Logger log = Logger.getLogger(NodeManager.class);

    private Map<String, Node> nodeMap; // remote address as key
    private Map<String, BloomFilter> nodeBloomFilterMap; // key as same as nodeMap
    private Map<String, Set<String>> nodeStoredFiles;
    public FileSystemTree fileSystemTree;

    public NodeManager() {
        nodeMap = new HashMap<>();
        nodeBloomFilterMap = new HashMap<>();
        nodeStoredFiles = new HashMap<>();
        fileSystemTree = new FileSystemTree();
    }

    public void addFilePathTiFileSystemTree(String filepath) {
        log.info("[FILE] add path: " + filepath);
        fileSystemTree.addPath(filepath);
    }

    public String getDirectories(String path) {
        return fileSystemTree.ls(path);
    }

    public List<String> getFilesUnder(String path) {
        return fileSystemTree.posixLs(path);
    }

    public void registerNode(Message.Node2Server.Registration registration, ChannelHandlerContext ctx) {
        String hostname = registration.getHostName();
        int port = registration.getHostPort();
        String ipAddress = ChannelHandlerContextUtils.getIpAddressFromChannel(ctx.channel());
        String remoteAddress = ChannelHandlerContextUtils.getRemoteAddressWithPort(ctx.channel());
        nodeMap.put(remoteAddress, new Node(hostname, ipAddress, port, ctx));
        nodeBloomFilterMap.put(remoteAddress, new BloomFilter(Constant.BLOOM_FILTER_FILTERS, Constant.BLOOM_FILTER_HASHES));
        nodeStoredFiles.put(remoteAddress, new HashSet<>());
        log.info("[Registration] " + remoteAddress + " registered");
    }

    public boolean removeNode(String nodeHost) {
        if (nodeMap.containsKey(nodeHost)) {
            nodeMap.remove(nodeHost);
            nodeBloomFilterMap.remove(nodeHost);
            nodeRecovery(nodeHost);
            log.info("[LOST] " + nodeHost + "removed");
            return true;
        }
        return false;
    }

    public void deleteFile(String hashedFilepath) {
        Message.Server2Node.FileDeletion fileDeletion = Message.Server2Node.FileDeletion.newBuilder().setFilepath(hashedFilepath).build();
        Message.Server2Node server2Node = Message.Server2Node.newBuilder().setFileDeletion(fileDeletion).build();
        Message.DFSMessagesWrapper dfs = Message.DFSMessagesWrapper.newBuilder().setServer2Node(server2Node).build();
        for (Node node : nodeMap.values()) {
            node.getCtx().channel().writeAndFlush(dfs).syncUninterruptibly();
        }
        log.info("[DELETION] Announce to delete " + hashedFilepath);
    }

    public void nodeRecovery(String nodeHost) {
        List<String> files = new ArrayList<>(nodeStoredFiles.remove(nodeHost));
        List<String> hosts = new ArrayList<>(nodeBloomFilterMap.keySet());
        log.info("Node connection lost detected. Replicas recovering process starts");
        BloomFilter bf;
        SecureRandom random = new SecureRandom();
        for (String filePath : files) {
            String fileDestination = null;
            String holdingNode = null;
            while (fileDestination == null || holdingNode == null) {
                String randomNodeHost = hosts.get(random.nextInt(nodeBloomFilterMap.size()));
                bf = nodeBloomFilterMap.get(randomNodeHost);
                boolean bfResult = bf.get(filePath.getBytes());
                if (holdingNode == null && bfResult) {
                    if (nodeStoredFiles.get(randomNodeHost).contains(filePath)) {
                        holdingNode = randomNodeHost;
                    }
                } else if (fileDestination == null && !bfResult) {
                    fileDestination = nodeMap.get(randomNodeHost).getIpWithPort();
                }
            }
            log.info("[RECOVERY] Instruct " + holdingNode + " to send " + filePath + " to " + fileDestination);
            Message.Server2Node.ReplicaRecovery replicaRecovery = Message.Server2Node.ReplicaRecovery.newBuilder()
                    .setDestination(fileDestination)
                    .setFilepath(filePath)
                    .build();
            Message.Server2Node server2Node = Message.Server2Node.newBuilder()
                    .setReplicaRecovery(replicaRecovery)
                    .build();
            Message.DFSMessagesWrapper dfsMessagesWrapper = Message.DFSMessagesWrapper.newBuilder()
                    .setServer2Node(server2Node)
                    .build();
            nodeMap.get(holdingNode).getCtx().channel().writeAndFlush(dfsMessagesWrapper).syncUninterruptibly();
        }
    }

    public String lookUpActiveNode() {
        if (nodeMap.isEmpty()) {
            return "No nodes are available now.";
        }
        StringBuilder sb = new StringBuilder();
        for (String nodeIp : nodeMap.keySet()) {
            Node node = nodeMap.get(nodeIp);
            String nodename = node.getHostname();
            String ip = node.getIpAddress();
            int port = node.getPort();
            String hostInfo = String.format("Host: %s, IP: %s, PORT: %d, Free space: %s, Processed number: %d\n",
                    nodename, ip, port, node.getFreeSpaceInGB(), node.getProcessedNumber());
            sb.append(hostInfo);
        }
        return sb.toString();
    }

    public List<Message.ChunkPlacementDecision> decideChunkPlacement(int chunkNumber) {
        int nodeNumber = nodeMap.keySet().size();
        long chunkSize = FileProcessUtil.sizeOfChunk;
        long fileSize = chunkNumber * chunkSize;
        List<Node> nodes = new ArrayList<>(nodeMap.values());
        long availableSize = nodes.stream().mapToLong(Node::getFreeSpace).sum();
        if (fileSize > availableSize) {
            return null;
        }
        List<Message.ChunkPlacementDecision> decision = new ArrayList<>(chunkNumber);
        for (int i = 1; i <= chunkNumber; i++) {
            if (nodeNumber == 1) {
                Node node = nodes.get(0);
                Message.ChunkPlacementDecision nodePlace = Message.ChunkPlacementDecision.newBuilder()
                        .setChunkId(i)
                        .setMainNode(node.getIpWithPort())
                        .setReplica1(node.getIpWithPort())
                        .setReplica2(node.getIpWithPort())
                        .build();
                decision.add(nodePlace);
            } else if (nodeNumber == 2) {
                nodes.sort((o1, o2) -> (int) (o2.getFreeSpace() - o1.getFreeSpace()));
                Node node1 = nodes.get(0);
                Node node2 = nodes.get(1);
                Message.ChunkPlacementDecision nodePlace = Message.ChunkPlacementDecision.newBuilder()
                        .setChunkId(i)
                        .setMainNode(node1.getIpWithPort())
                        .setReplica1(node2.getIpWithPort())
                        .setReplica2(node1.getIpWithPort())
                        .build();
                decision.add(nodePlace);
            } else { // nodeNumber >= 3
                nodes.sort((o1, o2) -> (int) (o2.getFreeSpace() - o1.getFreeSpace()));
                Node node1 = nodes.get(0);
                Node node2 = nodes.get(1);
                Node node3 = nodes.get(2);
                Message.ChunkPlacementDecision nodePlace = Message.ChunkPlacementDecision.newBuilder()
                        .setChunkId(i)
                        .setMainNode(node1.getIpWithPort())
                        .setReplica1(node2.getIpWithPort())
                        .setReplica2(node3.getIpWithPort())
                        .build();
                decision.add(nodePlace);
                node1.setFreeSpace(node1.getFreeSpace() - chunkSize);
                node2.setFreeSpace(node2.getFreeSpace() - chunkSize);
                node3.setFreeSpace(node3.getFreeSpace() - chunkSize);
            }
        }
        return decision;
    }

    public boolean updateNode(String host, long freeSpace, long processedNumber) {
        Node node = nodeMap.get(host);
        if (node == null) {
            return false;
        }
        node.setFreeSpace(freeSpace);
        node.setProcessedNumber(processedNumber);
        return true;
    }

    public void putBloomFilter(String host, String text) {
        if (nodeBloomFilterMap.containsKey(host)) {
            nodeBloomFilterMap.get(host).put(text.getBytes());
            log.info("[BF] put " + text);
        }
    }

    public List<String> getFilePossiblePosition(String text) {
        List<String> possiblePositions = new ArrayList<>();
        log.info("[BF] get " + text);
        for (String host : nodeBloomFilterMap.keySet()) {
            Node node = nodeMap.get(host);
            BloomFilter bf = nodeBloomFilterMap.get(host);
            if (bf.get(text.getBytes())) {
                possiblePositions.add(node.getIpWithPort());
            }
        }
        return possiblePositions;
    }

    public void putNodeStoredFiles(String host, String filepath) {
        nodeStoredFiles.get(host).add(filepath);
    }
}

class Node {
    private String hostname; // node's nickname, like node1/localhost
    private int port;   // like 5566
    private String ipAddress; // like 127.0.0.1
    private ChannelHandlerContext ctx;
    private long processedNumber; // updated by heartbeat
    private long freeSpace;   // updated by heartbeat

    public Node(String hostname, String ipAddress, int port, ChannelHandlerContext ctx) {
        this.hostname = hostname;
        this.ipAddress = ipAddress;
        this.port = port;
        this.ctx = ctx;
    }

    public String getFreeSpaceInGB() {
        return String.format("%.3f GB", getFreeSpaceInDouble() / (1024.0 * 1024.0 * 1024.0));
    }

    public double getFreeSpaceInDouble() {
        File file = new File("/");
        return file.getFreeSpace();
    }

    @Override
    public String toString() {
        return String.format("Node{name: %s, address: %s, port: %d}", hostname, ipAddress, port);
    }

    public long getProcessedNumber() {
        return processedNumber;
    }

    public void setProcessedNumber(long processedNumber) {
        this.processedNumber = processedNumber;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getIpWithPort() {
        return getIpAddress() + ":" + getPort();
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }
}