package qengine.program;

public class BPlusTreeNode {

    private Integer value;
    private BPlusTreeNode[] children;
    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public BPlusTreeNode[] getChildren() {
        return children;
    }

    public void setChildren(BPlusTreeNode[] children) {
        this.children = children;
    }

}
