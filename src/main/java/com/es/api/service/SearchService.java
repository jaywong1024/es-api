package com.es.api.service;

import com.alibaba.fastjson.JSON;
import com.es.api.pojo.JdContent;
import com.es.api.utils.ParserHTML;
import org.apache.tomcat.util.buf.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class SearchService {

    private static final String INDICES = "jd";

    private static final String HIGH_LIGHTER_TITLE = "title";
    private static final String HIGH_LIGHTER_SHOP = "shop";

    private static final String[] TAG = {"<span style='color: red;'>", "</span>"};

    @Autowired
    private ParserHTML parserHTML;

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 根据关键词前往 jd.com 爬取前 30 条数据，写入 ElasticSearch
     */
    public boolean addDocumentFromJd(String keyword) throws Exception {
        BulkRequest request = new BulkRequest(INDICES);
        request.timeout(TimeValue.timeValueSeconds(10));

//        解析数据
        List<JdContent> jdContentList = parserHTML.parserJd(keyword);

        Optional.ofNullable(jdContentList).ifPresent(u -> u.forEach(i -> {
            request.add(new IndexRequest().source(JSON.toJSONString(i), XContentType.JSON));
        }));

        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        return !responses.hasFailures();
    }

    /**
     * 查询爬取的数据
     */
    public List<Object> search(String keyword, Integer page, Integer size) throws IOException {
//        返回结果
        List<Object> result = new ArrayList<>();
//        查询请求
        SearchRequest request = new SearchRequest(INDICES);
//        搜索源构建器
        SearchSourceBuilder builder = new SearchSourceBuilder();
//        设置超时
        builder.timeout(TimeValue.timeValueSeconds(60));

//        搜索条件
//        must      字段必须包含
//        mushNot   字段必须不包含
//        should    字段可以包含
//        QueryBuilders.matchQuery() 和 new MatchQueryBuilder 是一样的
        builder.query(new BoolQueryBuilder()
                .must(QueryBuilders.matchQuery(HIGH_LIGHTER_TITLE, keyword))
                .should(new MatchQueryBuilder(HIGH_LIGHTER_SHOP, keyword))
        );


//        高亮设置
        builder.highlighter(new HighlightBuilder()
//                高亮字段
                .field(HIGH_LIGHTER_TITLE)
                .field(HIGH_LIGHTER_SHOP)
//                高亮样式
                .preTags(TAG[0]).postTags(TAG[1])
//                如果要多个字段高亮，这项要为 false
                .requireFieldMatch(false));

//        分页
        builder.from(page).size(size);

//        搜索
        SearchResponse response = client.search(request.source(builder), RequestOptions.DEFAULT);

        for (SearchHit hit : response.getHits().getHits()) {
//            处理高亮结果
            setHighLighter(hit, HIGH_LIGHTER_TITLE);
            setHighLighter(hit, HIGH_LIGHTER_SHOP);

            result.add(hit.getSourceAsMap());
        }
        return result;
    }

    /**
     * 处理高亮结果
     *
     * @param hit            查询结果
     * @param highlightField 高亮字段名称
     */
    private void setHighLighter(SearchHit hit, String highlightField) {
        Optional.ofNullable(hit.getHighlightFields().get(highlightField)).ifPresent(title -> {
            Text[] fragments = title.getFragments();
            List<String> fragmentStrArr = new ArrayList<>();
            for (Text fragment : fragments) {
                fragmentStrArr.add(fragment.toString());
                hit.getSourceAsMap().put(highlightField,
                        Optional.of(
                                StringUtils.join(fragmentStrArr, ' ')
                        ).orElse(
                                hit.getSourceAsMap().get(highlightField).toString()
                        )
                );
            }
        });
    }

}
