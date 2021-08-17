/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.theredlabs.deduper;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author erickrojas
 */
public class FolderParser implements Runnable {

    String basePath;
    private final BlockingQueue<String> fileQueue;
    ArrayList<String> pendingFiles;

    public FolderParser(String path, BlockingQueue fileQueue) {
        basePath = path;
        this.pendingFiles = new ArrayList<>();
        this.fileQueue = fileQueue;
    }
    
    protected void parse(String currentPath) {
        File dir = new File(currentPath);
        if (!dir.isDirectory()) {
            throw new RuntimeException("Supplied directory does not exist.");
        }

        for (File child : dir.listFiles()) {
            if (!child.getName().equals(".") && !child.getName().equals("..")) {
                if (child.isDirectory()) {
                    this.parse(child.getAbsolutePath());
                } else {
                    this.pendingFiles.add(child.getAbsolutePath());
//                    this.sendToFileQueue(child.getAbsolutePath());
                }
            }
        }        
    }
    
    @Override
    @SuppressWarnings("empty-statement")
    public void run() {
        this.parse(this.basePath);
        System.out.println("File list is complete");
        
        while (!this.pendingFiles.isEmpty()) {
            String nextFile = this.pendingFiles.get(0);
            while (!this.fileQueue.offer(nextFile));
            this.pendingFiles.remove(0);
        }
    }

    public void dump(boolean showFiles) {
        if (showFiles) {
            this.pendingFiles.forEach(file -> {
                System.out.println(file);
            });
        }
        System.out.println("Total Files: " + this.pendingFiles.size());
    }
    
    protected boolean sendToFileQueue(String filePath)
    {
        if (!this.fileQueue.offer(filePath)) {
            this.pendingFiles.add(filePath);
            return false;
        }
        return true;
    }
}
