package edu.usfca.cs.dfs.node;

import edu.usfca.cs.dfs.proto.Message;

import java.io.File;
import java.util.*;

public class FileSystemTree {
    private DfsDirectory root = new DfsDirectory("/");

    public void addPath(String filePath) {
        root.addKid(filePath);
    }

    public String ls(String filepath) {
        List<String> dirs = new ArrayList<>(Arrays.asList(filepath.split("/")));
        dirs.removeIf(String::isEmpty);
        DfsDirectory dfsDirectory = root;

        for(String dir : dirs){
            if(!dfsDirectory.directoryMap.containsKey(dir)) return "no such directory\n";

            dfsDirectory = dfsDirectory.directoryMap.get(dir);
        }

        StringBuilder sb = new StringBuilder();
        for (String str : dfsDirectory.getSubDirsStrings()) {
            sb.append(str).append("/\n");
        }
        return sb.toString();
    }

    public List<String> posixLs(String filepath) {
        List<String> dirs = new ArrayList<>(Arrays.asList(filepath.split("/")));
        dirs.removeIf(String::isEmpty);
        DfsDirectory dfsDirectory = root;

        for(String dir : dirs){
            if(!dfsDirectory.directoryMap.containsKey(dir)) return null;

            dfsDirectory = dfsDirectory.directoryMap.get(dir);
        }

        return dfsDirectory.getKidsStrings();
    }

    public boolean posixCheckIfExist(String filepath) {
        List<String> dirs = new ArrayList<>(Arrays.asList(filepath.split("/")));
        dirs.removeIf(String::isEmpty);
        DfsDirectory dfsDirectory = root;

        for(int i = 0; i < dirs.size() - 1; i++){
            String dir = dirs.get(i);

            if(!dfsDirectory.directoryMap.containsKey(dir)) {
                return false;
            }

            dfsDirectory = dfsDirectory.directoryMap.get(dir);
        }

        String dir = dirs.get(dirs.size() - 1);

        return (dfsDirectory.directoryMap.containsKey(dir)) || (dfsDirectory.fileSet.containsKey(dir));
    }

    private static class DfsDirectory {

        String path;
        String currentLayerName;
        Hashtable<String, DfsDirectory> directoryMap;
        Hashtable<String, Integer> fileSet;

        public DfsDirectory(String path) {
            directoryMap = new Hashtable<>();
            fileSet = new Hashtable<>();

            if(path.length() == 0) {
                this.path = path;
                return;
            }
            if(path.charAt(0) == '/') path = path.substring(1);
            this.path = path;
            if(path.length() == 0) return;

            String[] paths = path.split("/", 2);
            //case1:is a end dictionary
            if(paths.length == 1) return;
            //case2: still has kids
            this.addKid(paths[1]);
        }

        public void addKid(String kidPath){
            if(kidPath.charAt(0) == '/') kidPath = kidPath.substring(1);
            if(kidPath.length() == 0) return;

            String[] paths = kidPath.split("/", 2);
            if(paths.length == 1){
                //it's a file
                fileSet.put(kidPath, 0);
            }
            else{
                //it's still a file
                if(directoryMap.containsKey(paths[0])){
                    directoryMap.get(paths[0]).addKid(paths[1]);
                } else{
                    DfsDirectory kid = new DfsDirectory(kidPath);
                    directoryMap.put(paths[0], kid);
                }
            }
        }

        public List<String> getKidsStrings() {
            List<String> res = new ArrayList<>(directoryMap.keySet());
            res.addAll(fileSet.keySet());

            return res;
        }

        public List<String> getSubDirsStrings() {
            return new ArrayList<>(directoryMap.keySet());
        }
    }
}


