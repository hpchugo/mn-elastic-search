package com.github.hpchugo.es;

import com.github.hpchugo.Constants;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller("/data")
public class DataEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(DataEndpoint.class);
    private final RestHighLevelClient client;

    public DataEndpoint(RestHighLevelClient client) {
        this.client = client;
    }

    @ExecuteOn(TaskExecutors.IO)
    @Get("/document/{id}")
    public String byId(@PathVariable("id") String documentId) throws IOException {

        var response = client.get(new GetRequest(Constants.INDEX, documentId), RequestOptions.DEFAULT);
        final String source = response.getSourceAsString();
        LOG.debug("Response /document/{} () => {}", documentId, source);
        return source;
    }


    @Get("/document/async/{id}")
    public CompletableFuture<String> byIdAsAsync(@PathVariable("id") String documentId) throws IOException {
        final CompletableFuture<String> whenDone = new CompletableFuture<>();
        var response = client.getAsync(new GetRequest(Constants.INDEX, documentId), RequestOptions.DEFAULT, new ActionListener<>(){

            @Override
            public void onResponse(GetResponse response) {
                String sourceAsString = response.getSourceAsString();
                LOG.debug("Response /document/{} () => {}", documentId, sourceAsString);
                whenDone.complete(sourceAsString);
            }

            @Override
            public void onFailure(Exception e) {
                whenDone.completeExceptionally(e);
            }
        });
        return whenDone;
    }


    @Get("/document/async/firstname/{search}")
    public CompletableFuture<String> byFirstNameAsAsync(@PathVariable("search") String search) throws IOException {
        final CompletableFuture<String> whenDone = new CompletableFuture<>();
        final SearchSourceBuilder source =  new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("first_name", search));
        client.asyncSearch().submitAsync(new SubmitAsyncSearchRequest(source, Constants.INDEX), RequestOptions.DEFAULT, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                SearchHit[] hits = asyncSearchResponse.getSearchResponse().getHits().getHits();
                List<String> response = Stream.of(hits).map(SearchHit::getSourceAsString).collect(Collectors.toList());
                LOG.debug("Response /document/{} () => {}", search, response);
                whenDone.complete(response.toString());
            }

            @Override
            public void onFailure(Exception e) {
                whenDone.completeExceptionally(e);
            }
        });

        return whenDone;
    }
}
