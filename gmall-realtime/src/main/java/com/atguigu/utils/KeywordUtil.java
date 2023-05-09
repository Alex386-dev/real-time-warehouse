package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword(String keyword) throws IOException {
        ArrayList<String> list = new ArrayList<>();
        //创建IK分词对象 IK_smart IK_max_word(字符可能会有重复)
        StringReader stringReader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        //取出切好的词
        Lexeme next = ikSegmenter.next();
        while (next != null){
            list.add(next.getLexemeText());
            next = ikSegmenter.next();
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("尚硅谷大数据Flink数仓"));
    }
}
