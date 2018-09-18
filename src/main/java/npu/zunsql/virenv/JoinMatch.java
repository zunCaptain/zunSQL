package npu.zunsql.virenv;

import java.util.List;
import java.util.Map;

/**
 * Created by Huper on 2017/11/14.
 */
public class JoinMatch {

    private List<Column> joinHead;
    private Map<Integer,Integer> joinUnder;

    public JoinMatch(List<Column> joinHead, Map<Integer, Integer> joinUnder){
        this.joinHead = joinHead;
        this.joinUnder = joinUnder;
    }

    public List<Column> getJoinHead() {
        return joinHead;
    }

    public void setJoinHead(List<Column> joinHead) {
        this.joinHead = joinHead;
    }

    public Map<Integer, Integer> getJoinUnder() {
        return joinUnder;
    }

    public void setJoinUnder(Map<Integer, Integer> joinUnder) {
        this.joinUnder = joinUnder;
    }
}
