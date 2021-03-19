package edu.usfca.cs.dfs.posixClient;

import com.google.protobuf.ByteString;
import com.sun.security.auth.module.UnixSystem;
import edu.usfca.cs.dfs.basicClient.Client;
import edu.usfca.cs.dfs.proto.Message;
import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import org.apache.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClientFS extends FuseStubFS {

    private Client basicClient;

    private static final Logger logger = Logger.getLogger(ClientFS.class);

    private final String hostname;
    private final int port;
    private final String mountPoint;
    private Pattern pattern;

    private final String dirtyDir;

    //to accelerate
    private Hashtable<String, PosixFileKit> fileKitTable;

    private Hashtable<Pattern, Integer> hardCodeTable;

    private Hashtable<String, int[]> attrAccelerateTable;

    private Hashtable<String, Integer> retrievingTable;

    /**
     * Get all fileNames under "path"
     * for each name, you should do a filler.apply(buf, $NAME, null, 0));
     */
    @Override
    public int readdir(
            String path, Pointer buf, FuseFillDir filler,
            @off_t long offset, FuseFileInfo fi) {

        logger.info("[POSIX] Reading directory: " +  path);

        basicClient.posixLs(path);

        int round = 0;

        while(!basicClient.isPosixListPrepared() && round++ < 50){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        List<String> pathContentsList = basicClient.getPosixPathList();
        basicClient.setPosixListPrepared(false);

        if(pathContentsList != null){
            pathContentsList.forEach(
                    fileName -> filler.apply(buf, fileName, null, 0)
            );
        }

        /* These will always exist, but are filtered out on the server side: */
        filler.apply(buf, ".", null, 0);
        filler.apply(buf, "..", null, 0);

        return 0;
    }

    /**
     * What does it do? is 0 the correct status code?
     * @param path
     * @param fi
     * @return
     */
    @Override
    public int open(String path, FuseFileInfo fi) {
        //TODO: Should I do anything at here?

        return 0;
    }

    /**
     * Get attribute of a file/directory{
     *     message GetattrResponse {
     *     int32 status = 1;
     *     int32 mode = 2;
     *     int64 size = 3;
     * }
     * }
     */
    @Override
    public int getattr(String path, FileStat stat) {

        if(path.equals("/") || attrAccelerateTable.containsKey(path)){
            stat.st_mode.set(16832);
            stat.st_size.set(4224);

            /* Okay, so this is kind of an ugly hack. If you're mounting files on a
             * remote machines, the UIDs aren't going to match up, so we lie here
             * and say that all files are owned by the user on the CLIENT machine.
             * It would probably be better to pass in the UID from an external
             * launcher script, or just set up permissions to be readable by
             * everyone, although that has its own issues... */
            long uid = new UnixSystem().getUid();
            stat.st_uid.set(uid);

            //TODO: Handle the situations that status code is not 0(failed or some probelm)
            return 0;
        }

        for(Pattern pattern : hardCodeTable.keySet()){
            Matcher matcher = pattern.matcher(path);
            if(matcher.matches()){
                System.out.println("Dirty file");

                return -2;
            }
        }


        int mode = 0;
        long length = 0;

        logger.info("[POSIX] getattr: " +  path);

        basicClient.posixGetAttr(path);

        Message.ChunkMetadata chunkMetadata = null;

        int round = 0;

        while (chunkMetadata == null && round++ < 50) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            chunkMetadata = basicClient.posixHasAttrAndGet(path);
        }

        if(chunkMetadata != null){
            mode = chunkMetadata.getMode();
            length = chunkMetadata.getWholeFileSize();
        }

        stat.st_mode.set(mode);
        stat.st_size.set(length);

        /* Okay, so this is kind of an ugly hack. If you're mounting files on a
         * remote machines, the UIDs aren't going to match up, so we lie here
         * and say that all files are owned by the user on the CLIENT machine.
         * It would probably be better to pass in the UID from an external
         * launcher script, or just set up permissions to be readable by
         * everyone, although that has its own issues... */
        long uid = new UnixSystem().getUid();
        stat.st_uid.set(uid);

        //if it's a folder, skip
        if(mode == 16832 && length == 4224) {
            attrAccelerateTable.put(path, null);

            return 0;
        }

        //If current file is cached, but has a different size, set it to null
        if(fileKitTable.containsKey(path) && fileKitTable.get(path).length != length){
            //delete outdated file
            fileKitTable.remove(path);
        }

        return 0;
    }

    /**
     * get byte array of a file
     * @param path
     * @param buf
     * @param size
     * @param offset
     * @param fi
     * @return
     */
    @Override
    public int read(String path, Pointer buf,
            @size_t long size, @off_t long offset, FuseFileInfo fi) {

        logger.info("[POSIX] read: " + path);

        File file = null;

        if(retrievingTable.containsKey(path)){
            int round = 0;
            while (retrievingTable.containsKey(path) && round++ < 10) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if(fileKitTable.containsKey(path)){
            file = new File(fileKitTable.get(path).fileActualPath);
            System.out.println(file.getPath());
        }
        else file = readFileFromDFS(path);

        if(file == null){
            return -1;
        }

        byte[] contents = null;

        try (FileInputStream fin = new FileInputStream(file.getPath())) {
            fin.skip(offset);
            contents = new byte[(int) size];

            int readSize = fin.read(contents);

            fin.close();

            if(readSize <= 0) return readSize;

            buf.put(0, contents, 0, contents.length);
            return contents.length;
        } catch (IOException e) {
            logger.error("[POSIX] Error reading file", e);
            return 0;
        }
    }

    private File readFileFromDFS(String path){
        retrievingTable.put(path, 0);

        basicClient.posixReadFile(path);

        int round = 0;

        File file = null;


        while (file == null && round++ < 50) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            file = basicClient.posixGetReadFile(path);
        }

        PosixFileKit posixFileKit = new PosixFileKit(path, file.length());
        posixFileKit.fileActualPath = file.getPath();

        System.out.println(file.getPath());

        fileKitTable.put(path, posixFileKit);

        retrievingTable.remove(path);

        return file;
    }


    //TODO: Hard code INFO
    public ClientFS(String hostname, int port, String mountPoint) {
        this.hostname = hostname;
        this.port = port;

        this.basicClient = new Client(hostname, port);
        this.mountPoint = mountPoint;

        this.fileKitTable = new Hashtable<>();

        this.attrAccelerateTable = new Hashtable<>();
        this.retrievingTable = new Hashtable<>();

        this.hardCodeTable = new Hashtable<>();

        this.dirtyDir = ".*/lib.*so.*";
        this.pattern = Pattern.compile(this.dirtyDir);

        hardCodeTable.put(pattern, 0);
        hardCodeTable.put(Pattern.compile(".*/haswell.*"), 0);
        hardCodeTable.put(Pattern.compile(".*/x86_64.*"), 0);
        hardCodeTable.put(Pattern.compile(".*/tls.*"), 0);
    }

    public void start()
    throws IOException {
        try {
          //  logger.log(Level.INFO, "Mounting remote file system. "
             //       + "Press Ctrl+C or send SIGINT to quit.");

            this.mount(Paths.get(mountPoint), true, true);
        } finally {
            this.umount();
        }
    }
}
