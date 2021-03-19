package edu.usfca.cs.dfs.server;

import edu.usfca.cs.dfs.message.router.ClientMessageRouter;
import edu.usfca.cs.dfs.message.router.NodeMessageRouter;

import java.io.IOException;

public class Server {

    private NodeManager nodeManager = new NodeManager();
    private ClientMessageRouter clientMessageRouter;
    private NodeMessageRouter nodeMessageRouter;

    public Server(int clientPort, int nodePort) throws IOException {
        clientMessageRouter = new ClientMessageRouter(nodeManager);
        clientMessageRouter.start(clientPort);
        nodeMessageRouter = new NodeMessageRouter(nodeManager);
        nodeMessageRouter.start(nodePort);
    }
}
