/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.theredlabs.deduper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * @author erickrojas
 */
public class Main
{
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Database.connect();
        String rootFolder = (args.length == 1)?args[0]:".";
        System.out.println("Parsing: " + rootFolder);

        int cores = Runtime.getRuntime().availableProcessors();
        BlockingQueue<String> fileQueue = new ArrayBlockingQueue<>(cores);
        
        FolderParser parser = new FolderParser(rootFolder, fileQueue);
        FileHasher hasher = new FileHasher(fileQueue);
        Thread producer = new Thread(parser);
        Thread consumer = new Thread(hasher);
        producer.setName("File Parse");
        consumer.setName("File Hasher");
        producer.start();
        consumer.start();
    }
}
