package edu.usfca.cs.dfs.node;

public class NodeProcessRecorder {
    private int processedNumber;

    public int getProcessedNumber() {
        return processedNumber;
    }

    public void setProcessedNumber(int processedNumber) {
        this.processedNumber = processedNumber;
    }

    public void addProcessedNumber(int n) {
        processedNumber += n;
    }
}
