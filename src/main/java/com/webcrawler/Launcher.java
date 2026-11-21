package com.webcrawler;

import com.webcrawler.ui.WebCrawlerApp;

//Separate entry point avoids JavaFX module runtime check in JAR/jpackage
public final class Launcher {

    private Launcher() {
    }

    public static void main(String[] args) {
        WebCrawlerApp.launchApp(args);
    }
}
