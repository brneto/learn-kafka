/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.zinkworks.connectors.demo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        log.info("Running main...");
        System.out.println(new App().getGreeting());
    }
}