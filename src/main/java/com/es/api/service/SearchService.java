package com.es.api.service;

import com.alibaba.fastjson.JSON;
import com.es.api.pojo.JdContent;
import com.es.api.utils.ParserHTML;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
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

    private static final String HIGH_LIGHTER = "title";

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
        List<Object> result = new ArrayList<>();
        SearchRequest request = new SearchRequest(INDICES);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.timeout(TimeValue.timeValueSeconds(60));
        builder.query(new BoolQueryBuilder().must(new MatchQueryBuilder(HIGH_LIGHTER, keyword)))
                .highlighter(new HighlightBuilder().field(HIGH_LIGHTER)
                        .preTags(TAG[0]).postTags(TAG[1]))
                .from(page).size(size);
        SearchResponse response = client.search(request.source(builder), RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits().getHits()) {
//            将高亮字段设置回结果中
            hit.getSourceAsMap().put(HIGH_LIGHTER,
                    Optional.ofNullable(
                            hit.getHighlightFields().get(HIGH_LIGHTER).getFragments()[0].toString()
                    ).orElse(
                            hit.getSourceAsMap().get(HIGH_LIGHTER).toString()
                    )
            );
            result.add(hit.getSourceAsMap());
        }
        return result;
    }

}
