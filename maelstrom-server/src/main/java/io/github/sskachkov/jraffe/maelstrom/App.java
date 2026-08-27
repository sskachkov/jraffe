package io.github.sskachkov.jraffe.maelstrom;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        MaelstromServer server = new MaelstromServer(System.in, System.out);
        server.run();
    }
}
