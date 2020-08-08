package com.es.api.utils;

import com.es.api.pojo.JdContent;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 解析 HTML 工具类
 */
@Component
public class ParserHTML {

    /**
     * 解析京东网数据
     * @param keyword 关键词
     */
    public List<JdContent> parserJd(String keyword) throws Exception {
        List<JdContent> result = new ArrayList<>();
        String url = "https://search.jd.com/Search?keyword=" + keyword + "&enc=utf-8";
        Document document = Jsoup.parse(new URL(url), 3 * 1000);
        for (Element element : document.getElementById("J_goodsList").getElementsByTag("li")) {
            result.add(new JdContent(
                    element.getElementsByClass("p-name").eq(0).text(),
                    element.getElementsByClass("p-price").eq(0).text(),
//                    element.getElementsByTag("img").eq(0).attr("source-data-lazy-img")
                    element.getElementsByTag("img").eq(0).attr("src")
            ));
        }
        return result;
    }

}
