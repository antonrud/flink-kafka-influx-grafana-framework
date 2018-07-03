package de.tuberlin.tubit.gitlab.anton.rudacov.tools;

public class MorseTree {

    private Node root;


    public MorseTree() {

        this.root = new Node("root", null);

        this.root().setDot(new Node("E", this.root()));
        this.root().setDash(new Node("T", this.root()));

        this.root().dot().setDot(new Node("I", this.root().dot()));
        this.root().dot().setDash(new Node("A", this.root().dot()));
        this.root().dot().setDot(new Node("I", this.root().dot()));
        this.root().dot().setDot(new Node("I", this.root().dot()));


    }

    public Node root() {

        return root;
    }

    public static class Node {

        private final String data;
        private final Node parent;
        private Node dot;
        private Node dash;

        public Node(String data, Node parent) {

            this.data = data;
            this.parent = parent;
            this.dot = null;
            this.dash = null;
        }

        @Override
        public String toString() {

            return data;
        }

        public String getData() {
            return data;
        }

        public Node getParent() {
            return parent;
        }

        public Node dot() {
            return dot;
        }

        public void setDot(Node dot) {
            this.dot = dot;
        }

        public Node dash() {
            return dash;
        }

        public void setDash(Node dash) {
            this.dash = dash;
        }
    }
}
