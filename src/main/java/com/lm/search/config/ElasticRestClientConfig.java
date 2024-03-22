package com.lm.search.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import java.time.Duration;

/**
 * @author benjamin
 * @date 2022/10/1
 */
@Configuration
@EnableElasticsearchRepositories(basePackages = "com.lm.search")
public class ElasticRestClientConfig extends AbstractElasticsearchConfiguration {

    @Value("${spring.elasticsearch.uris}")
    private String url;
    @Value("${spring.elasticsearch.username}")
    private String username;
    @Value("${spring.elasticsearch.password}")
    private String password;

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        url = url.replace("http://","");
        String[] urlArr = url.split(",");
        HttpHost[] httpPostArr = new HttpHost[urlArr.length];
        for (int i = 0; i < urlArr.length; i++) {
            HttpHost httpHost = new HttpHost(urlArr[i].split(":")[0].trim(),
                    Integer.parseInt(urlArr[i].split(":")[1].trim()), "http");
            httpPostArr[i] = httpHost;
        }
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder = RestClient.builder(httpPostArr)
                // 异步httpclient配置
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    // 账号密码登录
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    // httpclient连接数配置
                    httpClientBuilder.setMaxConnTotal(30);
                    httpClientBuilder.setMaxConnPerRoute(10);
                    // httpclient保活策略
                    httpClientBuilder.setKeepAliveStrategy(((response, context) -> Duration.ofMinutes(5).toMillis()));
                    return httpClientBuilder;
                });
        return new RestHighLevelClient(builder);
    }

    @Bean
    public ElasticsearchRestTemplate elasticsearchRestTemplate(RestHighLevelClient elasticsearchClient, ElasticsearchConverter elasticsearchConverter){
        return new ElasticsearchRestTemplate(elasticsearchClient,elasticsearchConverter);
    }

}