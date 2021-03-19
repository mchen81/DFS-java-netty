package edu.usfca.cs.dfs;

import edu.usfca.cs.dfs.basicClient.Client;
import edu.usfca.cs.dfs.node.StorageNode;
import edu.usfca.cs.dfs.posixClient.ClientFS;
import edu.usfca.cs.dfs.server.Server;
import edu.usfca.cs.dfs.utils.Constant;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class DFSApp {

    public static void main(String[] args) throws IOException {
        try {
            String type = args[0];
            if (type.equals("server")) {
                int client = Integer.parseInt(args[1]);
                int node = Integer.parseInt(args[2]);
                Constant.FILEPATH_PREFIX += ("controller/");
                System.setProperty("LogPath", Constant.getLogFilePath());
                cleanUpTrashFiles();
                Server server = new Server(client, node);
            } else if (type.equals("client")) {
                String host = args[1];
                int port = Integer.parseInt(args[2]);
                Constant.FILEPATH_PREFIX += ("client/");
                System.setProperty("LogPath", Constant.getLogFilePath());
                cleanUpTrashFiles();
                Client client = new Client(host, port);
                client.start();
            } else if (type.equals("fuse")) {
                if (args.length != 4) {
                    System.err.println("Usage: ClientFS server port mount-point");
                    System.exit(1);
                }
                String host = args[1];
                int port = Integer.parseInt(args[2]);
                String mountPoint = args[3];
                System.out.println("host: " + host + ", port: " + port + "mountPoint: " + mountPoint);
                Constant.FILEPATH_PREFIX += ("fuse/");
                System.setProperty("LogPath", Constant.getLogFilePath());
                cleanUpTrashFiles();
                ClientFS clientFS = new ClientFS(host, port, mountPoint);
                clientFS.start();
            } else if (type.equals("storage")) {
                String hostname = args[1];
                String port = args[2];
                String nodeName = args[3];
                Constant.FILEPATH_PREFIX += (nodeName + "/");
                System.setProperty("LogPath", Constant.getLogFilePath());
                cleanUpTrashFiles();
                String nodePort = args[4];
                StorageNode storageNode = new StorageNode(hostname, Integer.parseInt(port), nodeName, Integer.parseInt(nodePort));
            } else {
                printOutUsage();
            }
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            printOutUsage();
        }
    }

    private static void printOutUsage() {
        System.out.println("DFS Usage");
        System.out.println("server [port for clients to connect] [port for nodes to connect]");
        System.out.println("client [server's ip] [server's port]");
        System.out.println("storage [server's ip] [server's port] [node name(could be any)] [node port for client to connect]");
        System.out.println("fuse [server's ip] [server's port] [mount point]");
    }

    private static void cleanUpTrashFiles() throws IOException {
        File file = new File(Constant.FILEPATH_PREFIX);
        if (!file.exists()) {
            file.mkdir();
        } else {
            FileUtils.cleanDirectory(file);
        }
    }
}
