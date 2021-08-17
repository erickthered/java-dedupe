/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.theredlabs.deduper;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author erickrojas
 */
public class FileHasher implements Runnable {

    private final BlockingQueue<String> fileQueue;
    private MessageDigest md;
    private long processedFiles = 0;
    public HashMap<String, List<String>> hashes = new HashMap<>();

    public String makeHashQuick(File infile) throws IOException {
        FileInputStream fin = new FileInputStream(infile);
        byte data[];
        if (infile.length() > 0) {
            data = new byte[(int) Math.min(
                infile.length(),
                (long) ((Integer.MAX_VALUE - 1)/ 2)
            )];
            fin.read(data);
            fin.close();
            String hash = new BigInteger(1, md.digest(data)).toString(16);
            return hash;
        }
        return "0";
    }

    public void dump() {
        for (String key : hashes.keySet()) {
            System.out.println(key + ":" + hashes.get(key));
        }
    }
    
    public long getFileCount() {
        return this.processedFiles;
    } 

    public void detectDuplicates() {
        int dupes = 0;
        int dupeKeys = 0;
        for (String key : hashes.keySet()) {
            List files = hashes.get(key);
            if (files.size() > 1) {
                System.out.println(files);
                dupes += files.size();
                dupeKeys++;
            }
        }
        System.out.println("Detected " + dupes + " duplicate files for " + dupeKeys + " duplicate keys");
    }

    public FileHasher(BlockingQueue<String> fileQueue) {
        this.fileQueue = fileQueue;
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("cannot initialize SHA-512 hash function", e);
        }
    }

    @SuppressWarnings("empty-statement")
    @Override
    public void run() {
        while (this.fileQueue.isEmpty());
        System.out.println("Processing Hashes");
        while (!this.fileQueue.isEmpty()) {
            try {
                String filePath = this.fileQueue.take();
                File file = new File(filePath);
                try {
                    String fileHash = makeHashQuick(file);
                    List<String> list = hashes.get(fileHash);
                    if (list == null) {
                        list = new LinkedList<>();
                        hashes.put(fileHash, list);
                    }
                    list.add(filePath);
                    this.processedFiles++;
                } catch (IOException e) {
                    System.err.println("Cannot read file: " + filePath);
                }
            } catch (InterruptedException e) {
                System.err.println("Consumer was interrupted");
            }
        }
        this.detectDuplicates();
        System.out.println(this.getFileCount() + " files analyzed");
    }
}
