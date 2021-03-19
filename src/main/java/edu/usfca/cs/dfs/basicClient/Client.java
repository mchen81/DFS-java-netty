package edu.usfca.cs.dfs.basicClient;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.message.sender.ClientMessageSender;
import edu.usfca.cs.dfs.message.sender.MessageSender;
import edu.usfca.cs.dfs.message.sender.ServerMessageSender;
import edu.usfca.cs.dfs.proto.Message;
import edu.usfca.cs.dfs.proto.Message.Server2Client.RetrieveFileResponse.ChunkPossiblePosition;
import edu.usfca.cs.dfs.utils.ChecksumUtil;
import edu.usfca.cs.dfs.utils.Constant;
import edu.usfca.cs.dfs.utils.FileProcessUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Client {

    static Logger log = Logger.getLogger(Client.class);

    private Hashtable<String, String> waitingFileMap = new Hashtable<>();
    private Hashtable<String, String> waitingRetrieveMap = new Hashtable<>();
    private Hashtable<String, String> waitingPreRetrieveMap = new Hashtable<>();
    private Hashtable<String, String> waitingMetadataTable = new Hashtable<>();
    private Hashtable<String, Message.ChunkMetadata> attrTable = new Hashtable<>();
    private Hashtable<String, String> posixRetrievedFileTable = new Hashtable<>();
    private Hashtable<String, Integer> posixRetrieveWaitingSet = new Hashtable<>();

    private static ExecutorService executor = Executors.newCachedThreadPool();

    private ServerMessageSender serverMessageHandler;
    private ClientMessageSender clientMessageSender;

    private boolean isPosixListPrepared;
    private List<String> posixPathList;

    private ProgressBar progressBar;

    public Client(String host, int port) {
        serverMessageHandler = new ServerMessageSender(this);
        serverMessageHandler.connectToServer(host, port);
        clientMessageSender = new ClientMessageSender(this);
        isPosixListPrepared = false;
        if (serverMessageHandler.isServerNull()) {
            throw new RuntimeException("Cannot connect to " + host + ":" + port);
        } else {
            System.out.println("Successfully connect to " + host + ":" + port);
            System.out.println("Client Usage: ");
            System.out.println("-save [local file path] [cluster file path]");
            System.out.println("-retrieve [cluster file path] [local file path (need to give a file name)]");
            System.out.println("-nodes => to see current active nodes");
            System.out.println("-ls [directory path] => to list directories under the given path");
        }
        File file = new File(Constant.FILEPATH_PREFIX);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    public void start() {
        InputReader reader = new InputReader(this);
        Thread inputThread = new Thread(reader);
        inputThread.start();
    }

    public void askStoreFile(String localFilePath, String clusterFilePath) {
        File file = new File(localFilePath);
        if (!clusterFilePath.endsWith("/")) {
            clusterFilePath += "/";
        }
        clusterFilePath += file.getName();
        log.info("[Client] Storing file : " + clusterFilePath);
        if (file.exists() && !waitingFileMap.containsKey(file.getName())) {
            //                byte[] data = FileUtils.readFileToByteArray(file);
            //                String checksum = ChecksumUtil.getMD5Checksum(data);
            int chunkNumber = FileProcessUtil.getChunkNumber(file.length());
            Message.FileMetadata fileMetadata = Message.FileMetadata.newBuilder()
                    .setFilePath(clusterFilePath)
                    .setChunkNumber(chunkNumber)
                    // .setChecksum(checksum)
                    .setSize(file.length())
                    .build();
            progressBar = new ProgressBar("Saving " + file.getName(), chunkNumber + 2);
            progressBar.step();
            serverMessageHandler.sendStoreFileRequest(fileMetadata);
            waitingFileMap.put(clusterFilePath, file.getPath());
        } else {
            System.out.println("File does not exist");
        }
    }

    public boolean isPosixListPrepared() {
        return isPosixListPrepared;
    }

    public void setPosixListPrepared(boolean posixListPrepared) {
        isPosixListPrepared = posixListPrepared;
    }

    public List<String> getPosixPathList() {
        return posixPathList;
    }

    public void setPosixPathList(List<String> posixPathList) {
        this.posixPathList = posixPathList;
    }

    public void sendChunksToNodes(String filePath, List<Message.ChunkPlacementDecision> serverDecision) {
        if (!waitingFileMap.containsKey(filePath)) {
            System.out.println("Cannot find the request");
            progressBar.close();
            return;
        }
        progressBar.step();
        File file = new File(waitingFileMap.get(filePath));
        Set<PosixFilePermission> permissions = null;
        try {
            permissions = Files.getPosixFilePermissions(
                    file.toPath(), LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            System.out.println("Permission denied");
            progressBar.close();
            return;
        }
        int mode = 0;
        if (permissions != null) {
            for (PosixFilePermission perm : PosixFilePermission.values()) {
                mode = mode << 1;
                mode += permissions.contains(perm) ? 1 : 0;
            }
        }
        long size = file.length();
        if (file.isDirectory()) {
            mode = mode | FileStat.S_IFDIR;
        } else {
            mode = mode | FileStat.S_IFREG;
        }
        try {
            ArrayList<String> chunkNameStrings = FileProcessUtil.split(file); // file in byte
            Map<Integer, Message.ChunkPlacementDecision> decisionMap = new HashMap<>(); // key: chunkId, value: chunk's placement decision
            Set<String> connectionHostSet = new HashSet<>(); // the connection will be established
            // prepare pre-process objects
            for (Message.ChunkPlacementDecision decision : serverDecision) {
                decisionMap.put(decision.getChunkId(), decision);
                connectionHostSet.add(decision.getMainNode());
            }
            int chunkNumber = chunkNameStrings.size();
            // pre-connect needed nodes
            ChunksSender chunksSender = new ChunksSender(chunkNumber, connectionHostSet);
            chunksSender.startNodesConnections();
            for (int id = 1; id <= chunkNameStrings.size(); id++) {
                File chunkFile = new File(chunkNameStrings.get(id - 1));
                byte[] buffer = new byte[(int) chunkFile.length()];
                try {
                    FileInputStream fileInputStream = new FileInputStream(chunkFile);
                    fileInputStream.read(buffer);
                    fileInputStream.close();
                } catch (Exception e) {
                    System.out.println("Fail to read file");
                    progressBar.close();
                    return;
                }
                ByteString chunk = ByteString.copyFrom(buffer, 0, buffer.length);
                //ByteString chunk = byteStrings.get(id - 1);
                String checksum = ChecksumUtil.getMD5Checksum(chunk.toByteArray());
                Message.ChunkMetadata metadata = Message.ChunkMetadata.newBuilder()
                        .setFilePath(filePath)
                        .setChunkId(id)
                        .setPlacement(decisionMap.get(id))
                        .setSize(chunk.size())
                        .setChecksum(checksum)
                        .setChunkTotalNumber(chunkNumber)
                        .setWholeFileSize(size)
                        .setMode(mode)
                        .build();
                Message.Client2Node.StoreChunkRequest storeChunkRequest = Message.Client2Node.StoreChunkRequest.newBuilder()
                        .setMetadata(metadata)
                        .setChunkBytes(chunk)
                        .build();
                Thread thread = new Thread(new ChunkSendWorker(chunksSender, storeChunkRequest));
                executor.execute(thread);
            }
            while (!chunksSender.isFinished()) {
                progressBar.stepTo(chunksSender.getSentChunkNumber() + 2);
                TimeUnit.SECONDS.sleep(1);
            }
            progressBar.stepTo(progressBar.getMax());
            progressBar.close();
            chunksSender.disconnectAll();
            System.out.println(filePath + " saved.");
            FileUtils.cleanDirectory(new File(Constant.getTmpFilePath()));
            waitingFileMap.remove(filePath);
        } catch (InterruptedException e) {
            System.out.println("Error happened when sending chunks");
            progressBar.close();
        } catch (IOException e) {
            System.out.println("Error happend when clean up tmp dirs");
            progressBar.close();
        }
    }

    public void sendRetrieveFileRequestStep1(String filepath, String localDestination, boolean getAttr, boolean isFromFuse) {
        // input: -retrieve dir/file.txt
        // 1. ask controller: does the file exist?
        //case1: retrieve file
        if (!getAttr) {
            waitingPreRetrieveMap.put(filepath, localDestination);
        }
        //otherwise, getAttr
        else {
            waitingMetadataTable.put(filepath, "null");
        }
        serverMessageHandler.sendRetrieveFileRequestStep1(filepath, isFromFuse); // dir/file.txt
        log.info("[SendRetrieveFileRequest]" + filepath + ", " + localDestination);
        // 2. waiting for controller... -> retrieveFile()
    }

    public void sendRetrieveFileRequestStep2(String filepath, int chunkNumber, String localDestination) {
        // input: -retrieve dir/file.txt
        // 1. ask controller: does the file exist?
        waitingRetrieveMap.put(filepath, localDestination);
        serverMessageHandler.sendRetrieveFileRequestStep2(filepath, chunkNumber); // dir/file.txt
        // 2. waiting for controller... -> retrieveFile()
    }

    public void receiveChunkMetadataHandler(Message.Node2Client.RetrieveChunkRespond retrieveChunkRespond) {
        Message.ChunkMetadata chunkMetadata = retrieveChunkRespond.getMetadata();
        String filePath = chunkMetadata.getFilePath();
        if (waitingMetadataTable.containsKey(filePath)) {
            attrTable.put(filePath, chunkMetadata);
            waitingMetadataTable.remove(filePath);
        }
        if (waitingPreRetrieveMap.containsKey(filePath)) {
            String localDestination = waitingPreRetrieveMap.remove(filePath);
            sendRetrieveFileRequestStep2(filePath, chunkMetadata.getChunkTotalNumber(), localDestination);
        }
    }

    public void preRetrieveFile(Message.Server2Client.RetrieveFileResponse response) {
        String filePath = response.getFilePath();
        if (!response.getSuccess()) {
            //Again, our super Fuse repeatedly ask my baby DFS some files that does not exist.
            System.out.println(filePath + " does not exist in cluster");
            waitingPreRetrieveMap.remove(filePath);
            if (waitingMetadataTable.containsKey(filePath)) {
                waitingMetadataTable.remove(filePath);
                Message.ChunkMetadata fakeMetadata = Message.ChunkMetadata.newBuilder()
                        .setMode(-2)
                        .setWholeFileSize(-2)
                        .build();
                attrTable.put(filePath, fakeMetadata);
            }
            return;
        }
        List<ChunkPossiblePosition> cppList = response.getChunkPossiblePositionList();
        if (cppList == null || cppList.size() != 1) {
            System.out.println(filePath + " does not exist in cluster");
            waitingPreRetrieveMap.remove(filePath);
            waitingMetadataTable.remove(filePath);
            return;
        }
        ChunkPossiblePosition cpp = cppList.get(0);
        int hostCount = cpp.getHostCount();
        int chunkId = cpp.getChunkId();
        int hostNum = 0;
        if (hostCount == 0 || cpp.getHostList().size() == 0) {
            //file/chunk did not found
            System.out.println(filePath + " does not exist in cluster");
            waitingPreRetrieveMap.remove(filePath);
            if (waitingMetadataTable.containsKey(filePath)) {
                //Ohh... Of course, we still have to give posix some info to let Earth keep rotating
                Message.ChunkMetadata fakeMetadata = Message.ChunkMetadata.newBuilder()
                        .setMode(16832)
                        .setWholeFileSize(4224)
                        .build();
                attrTable.put(filePath, fakeMetadata);
                waitingMetadataTable.remove(filePath);
            }
            return;
        }
        if (!waitingPreRetrieveMap.containsKey(filePath) && !waitingMetadataTable.containsKey(filePath)) {
            System.out.println("Fail to retrieve " + filePath);
            return;
        }
        ArrayList<String> hostList = new ArrayList<>(cpp.getHostList());
        Message.Client2Node.RetrieveChunkRequest request = Message.Client2Node.RetrieveChunkRequest.newBuilder()
                .setChunkId(1)
                .setFilePath(filePath)
                .setChunkMetadataOnly(true)
                .build();
        Message.Client2Node client2Node = Message.Client2Node.newBuilder()
                .setRetrieveChunkRequest(request)
                .build();
        Message.DFSMessagesWrapper wrp = Message.DFSMessagesWrapper.newBuilder()
                .setClient2Node(client2Node)
                .build();
        for (String ipWithPort : hostList) {
            System.out.println("preRetrieveFile: " + ipWithPort);
            clientMessageSender.sendRequest(ipWithPort, wrp);
        }
    }

    public void retrieveFile(Message.Server2Client.RetrieveFileResponse response) {
        // 1. controller says yes and returns possible nodes
        if (!response.getSuccess()) {
            System.out.println("No such file in server");
            return;
        }
        Message.FileMetadata metadata = response.getMetadata();
        String filepath = metadata.getFilePath();
        if (!waitingRetrieveMap.containsKey(filepath)) {
            System.out.println("No such file in local");
            return;
        }
        List<ChunkPossiblePosition> chunkPossiblePositions = response.getChunkPossiblePositionList();
        // 2. open threads to touch the nodes and get chunks
        // 2.1 open pool
        // 2.2 add thread ChunkReceiver to pool
        boolean[] chunksReceived = new boolean[metadata.getChunkNumber()];
        ArrayList<Future<String>> futureList = new ArrayList<>();
        Hashtable<String, HashSet<Integer>> nodeChunkTable = new Hashtable<>();
        for (ChunkPossiblePosition cpp : chunkPossiblePositions) {
            int chunkId = cpp.getChunkId();
            // String chunkName = filepath + chunkId;
            for (String potentialPosition : cpp.getHostList()) {
                HashSet<Integer> set = nodeChunkTable.getOrDefault(potentialPosition, new HashSet<>());
                set.add(chunkId);
                // if (chunkId == 10) System.out.println("10: " + potentialPosition);
                nodeChunkTable.put(potentialPosition, set);
            }
        }
        for (String address : nodeChunkTable.keySet()) {
            futureList.add(executor.submit(new ChunkReceiver(chunksReceived, metadata.getFilePath()
                    , nodeChunkTable.get(address), address), "DONE"));
        }
        int chunkNum = metadata.getChunkNumber();
        progressBar = new ProgressBar("Retrieving " + filepath, chunkNum + 2);
        progressBar.step();
        HashSet<Future<String>> finishSet = new HashSet<>();
        int round = 0;
        while (finishSet.size() < futureList.size() && round < 180) {
            for (Future<String> future : futureList) {
                if (future.isDone()) {
                    if (!finishSet.contains(future)) {
                        finishSet.add(future);
                        progressBar.stepTo(finishSet.size() + 1);
                    }
                }
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.out.println("Error happened when retrieving chunks");
            }
            round++;
        }
        round = 0;
        HashSet<Integer> finishChunkIDSet = new HashSet<>();
        while (finishChunkIDSet.size() < chunkNum && round < 180) {
            for (int i = 0; i < chunksReceived.length; i++) {
                if (chunksReceived[i]) {
                    finishChunkIDSet.add(i);
                }
            }
            try {
                progressBar.stepTo(finishChunkIDSet.size() + 1);
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                progressBar.close();
                System.out.println("Error happened when retrieving chunks");
            }
            round++;
        }
        //=================================================================================
        // 3. reconstruct files by the chunks
        for (int i = 0; i < chunksReceived.length; i++) {
            if (!chunksReceived[i]) {
                progressBar.close();
                System.out.printf("Fail to retrieve file: Chunk %d is missed.\n", (i + 1));
                return;
            }
        }
        String destinationFilename = waitingRetrieveMap.remove(filepath);
        log.info("[STORE] filename: " + destinationFilename);
        try {
            File desFile = new File(destinationFilename);
            if (desFile.exists()) {
                FileUtils.deleteQuietly(desFile);
            } else {
                FileUtils.touch(desFile);
            }
            FileProcessUtil.reconstructChunksToFile(chunksReceived.length, desFile);
            progressBar.stepTo(progressBar.getMax());
            progressBar.close();
            System.out.println("File retrieved successfully");
            FileUtils.cleanDirectory(new File(Constant.getTmpFilePath()));
            if (posixRetrieveWaitingSet.containsKey(filepath)) {
                posixRetrieveWaitingSet.remove(filepath);
                //File posixTargetFile = new File(destinationFilename);
                posixRetrievedFileTable.put(filepath, destinationFilename);
            }
        } catch (IOException e) {
            System.out.println("Fail to retrieve file");
            System.out.println(e);
            System.out.println(destinationFilename);
            log.info(e);
        }
    }

    public void askActiveNodes() {
        Message.Client2Server.Command command = Message.Client2Server.Command.newBuilder()
                .setCommand("-nodes")
                .build();
        serverMessageHandler.sendCommand(command);
    }

    public void receiveCommandResult(String result) {
        System.out.println(result);
    }

    public void askListFileSystem(String path) {
        Message.Client2Server.Command command = Message.Client2Server.Command.newBuilder()
                .setCommand("-ls")
                .setText(path)
                .build();
        serverMessageHandler.sendCommand(command);
    }

    /**
     * for posix ls
     */
    public void posixLs(String path) {
        isPosixListPrepared = false;
        Message.Client2Server.Command command = Message.Client2Server.Command.newBuilder()
                .setCommand("-lsPosix")
                .setText(path)
                .build();
        serverMessageHandler.sendCommand(command);
    }

    /**
     * for posix getAttr
     */
    public void posixGetAttr(String path) {
        sendRetrieveFileRequestStep1(path, null, true, true);
    }

    public Message.ChunkMetadata posixHasAttrAndGet(String path) {
        if (attrTable.containsKey(path)) {
            return attrTable.remove(path);
        } else return null;
    }

    /**
     * for posix readFile
     * TODO:
     */
    public void posixReadFile(String path) {
        //        if(path.charAt(0) == '/') path = path.substring(1)
        posixRetrieveWaitingSet.put(path, 0);
        String des;
        if (path.charAt(0) == '/') des = Constant.FILEPATH_PREFIX + path.substring(1);
        else des = Constant.FILEPATH_PREFIX + path;
        sendRetrieveFileRequestStep1(path, des, false, false);
    }

    public File posixGetReadFile(String path) {
        return new File(posixRetrievedFileTable.remove(path));
    }

    private static class InputReader implements Runnable {
        private Client client;

        public InputReader(Client client) {
            this.client = client;
        }

        public void run() {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String line = "";
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
                if (line != null && line.length() > 0) {
                    handleLine(line);
                }
            }
        }

        private void handleLine(String line) {
            String[] lineArray = line.split("\\s+");
            if (lineArray.length == 0) return;
            //case1: save request
            if (lineArray[0].equals("-save")) {
                if (lineArray.length < 3) {
                    System.out.println("[Usage] -save [local file path] [cluster filepath]");
                    return;
                }
                String localFilePath = lineArray[1];
                File file = new File(localFilePath);
                if (!file.exists()) {
                    System.out.println(localFilePath + " does not exist.");
                    return;
                }
                String clusterFilePath = lineArray[2];
                client.askStoreFile(localFilePath, clusterFilePath);
            } else if (lineArray[0].equals("-retrieve")) {
                if (lineArray.length < 3) {
                    System.out.println("[Usage] -retrieve [cluster file path] [local file path]");
                    return;
                }
                String filepath = lineArray[1];
                String destination = lineArray[2];
                File file = new File(destination);
                if (file.isDirectory()) {
                    System.out.println(destination + " is not a file path");
                    return;
                }
                client.sendRetrieveFileRequestStep1(filepath, destination, false, false);
            } else if (lineArray[0].equals("-nodes")) {
                log.info("-nodes receive");
                client.askActiveNodes();
            } else if (lineArray[0].equals("-ls")) {
                if (lineArray.length < 2) {
                    System.out.println("[Usage] -ls [dfs directory]");
                    return;
                }
                String path = lineArray[1];
                client.askListFileSystem(path);
            } else {
                System.out.println("Unknown command, please input again");
            }
        }
    }

    @ChannelHandler.Sharable
    class ChunkReceiver extends MessageSender implements Runnable {

        private boolean[] chunksReceived;
        private Channel nodeChannel;
        private String filePath;
        private volatile boolean isFileReceived;
        private String address;
        private HashSet<Integer> chunkIdSet;
        private HashSet<Integer> receivedSet;

        public ChunkReceiver(boolean[] chunksReceived,
                             String filePath, HashSet<Integer> chunkIdSet, String address) {
            this.chunksReceived = chunksReceived;
            this.filePath = filePath;
            this.chunkIdSet = chunkIdSet;
            this.address = address;
            receivedSet = new HashSet<>();
        }

        @Override
        public void run() {
            Channel channel = connectTo(address);
            Message.Client2Node.RetrieveChunkRequest.Builder requestB = Message.Client2Node.RetrieveChunkRequest.newBuilder()
                    .setFilePath(filePath)
                    .setChunkMetadataOnly(false);
            Message.Client2Node.Builder client2NodeB = Message.Client2Node.newBuilder();
            Message.DFSMessagesWrapper.Builder wrpB = Message.DFSMessagesWrapper.newBuilder();
            for (Integer chunkId : chunkIdSet) {
                if (chunksReceived[chunkId - 1]) continue;
                Message.Client2Node.RetrieveChunkRequest retrieveChunkRequest = requestB.setChunkId(chunkId).build();
                Message.Client2Node client2Node = client2NodeB.setRetrieveChunkRequest(retrieveChunkRequest).build();
                Message.DFSMessagesWrapper wrapper = wrpB.setClient2Node(client2Node).build();
                channel.writeAndFlush(wrapper);
            }
            //As it's a single thread, we can wait a bit longer
            int round = 45;
            while (receivedSet.size() < chunkIdSet.size() && round-- > 0) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("Error happened when retrieving file");
                }
                boolean finished = true;
                for (boolean b : chunksReceived) {
                    finished = finished && b;
                }
                if (finished) break;
            }
            assert nodeChannel != null;
            nodeChannel.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.DFSMessagesWrapper dfs) throws Exception {
            if (dfs.hasNode2Client()) {
                Message.Node2Client node2Client = dfs.getNode2Client();
                Message.Node2Client.RetrieveChunkRespond respond = node2Client.getRetrieveChunkRespond();
                log.info("Chunk received" + respond.getMetadata().getChunkId());
                if (respond.getHasFile()) {
                    int id = respond.getMetadata().getChunkId();
                    receivedSet.add(id);
                    if (chunksReceived[id - 1]) {
                        return;
                    }
                    chunksReceived[id - 1] = true;
                    try {
                        FileUtils.writeByteArrayToFile(new File(Constant.getTmpFilePath() + id), respond.getChunk().toByteArray());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static class ChunkSendWorker implements Runnable {
        private ChunksSender chunksSender;
        private Message.Client2Node.StoreChunkRequest request;

        public ChunkSendWorker(ChunksSender chunksSender, Message.Client2Node.StoreChunkRequest request) {
            this.chunksSender = chunksSender;
            this.request = request;
        }

        @Override
        public void run() {
            chunksSender.sendChunk(request);
        }
    }
}

