package com.lm.search.config;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.IndexOperations;

import javax.annotation.Resource;
import java.util.Set;

// @Configuration
@Slf4j
@AllArgsConstructor
public class ElasticCreateIndexStartUp implements ApplicationListener {

    @Resource
    private final ElasticsearchRestTemplate restTemplate;
    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent){
        log.info("[elastic]索引初始化...");
        Reflections f = new Reflections("com.yupi.yuso.model.entity");
        Set<Class<?>> classSet = f.getTypesAnnotatedWith(Document.class);
        for (Class clazz : classSet) {
            IndexOperations indexOperations = restTemplate.indexOps(clazz);
            if(!indexOperations.exists()){
                indexOperations.create();
                indexOperations.putMapping();
                log.info(String.format("[elastic]索引%s数据结构创建成功",clazz.getSimpleName()));
            }
        }
        log.info("[elastic]索引初始化完毕");
    }

}