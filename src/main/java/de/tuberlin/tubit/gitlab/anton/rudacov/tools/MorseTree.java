package de.tuberlin.tubit.gitlab.anton.rudacov.tools;


/* DO NOT ALTER THIS CLASS */

public class MorseTree {

    private Node root;

    public MorseTree() {

        this.root = new Node("root", null);

        this.root().setDot(new Node("E", this.root()));
        this.root().setDash(new Node("T", this.root()));

        this.root().dot().setDot(new Node("I", this.root().dot()));
        this.root().dot().setDash(new Node("A", this.root().dot()));
        this.root().dash().setDot(new Node("N", this.root().dash()));
        this.root().dash().setDash(new Node("M", this.root().dash()));

        this.root().dot().dot().setDot(new Node("S", this.root().dot().dot()));
        this.root().dot().dot().setDash(new Node("U", this.root().dot().dot()));
        this.root().dot().dash().setDot(new Node("R", this.root().dot().dash()));
        this.root().dot().dash().setDash(new Node("W", this.root().dot().dash()));

        this.root().dash().dot().setDot(new Node("D", this.root().dash().dot()));
        this.root().dash().dot().setDash(new Node("K", this.root().dash().dot()));
        this.root().dash().dash().setDot(new Node("G", this.root().dash().dash()));
        this.root().dash().dash().setDash(new Node("O", this.root().dash().dash()));

        this.root().dot().dot().dot().setDot(new Node("H", this.root().dot().dot().dot()));
        this.root().dot().dot().dot().setDash(new Node("V", this.root().dot().dot().dot()));
        this.root().dot().dot().dash().setDot(new Node("F", this.root().dot().dot().dash()));

        this.root().dot().dash().dot().setDot(new Node("L", this.root().dot().dash().dot()));
        this.root().dot().dash().dash().setDot(new Node("P", this.root().dot().dash().dash()));
        this.root().dot().dash().dash().setDash(new Node("J", this.root().dot().dash().dash()));

        this.root().dash().dot().dot().setDot(new Node("B", this.root().dash().dot().dot()));
        this.root().dash().dot().dot().setDash(new Node("X", this.root().dash().dot().dot()));
        this.root().dash().dot().dash().setDot(new Node("C", this.root().dash().dot().dash()));
        this.root().dash().dot().dash().setDash(new Node("Y", this.root().dash().dot().dash()));

        this.root().dash().dash().dot().setDot(new Node("Z", this.root().dash().dash().dot()));
        this.root().dash().dash().dot().setDash(new Node("Q", this.root().dash().dash().dot()));
    }

    // Returns root of the tree
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

        public Node dash() {
            return dash;
        }

        private void setDot(Node dot) {
            this.dot = dot;
        }

        private void setDash(Node dash) {
            this.dash = dash;
        }
    }
}
