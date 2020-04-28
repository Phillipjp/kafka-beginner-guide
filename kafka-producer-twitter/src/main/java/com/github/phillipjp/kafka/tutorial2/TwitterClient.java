package com.github.phillipjp.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient {

    private Client hosebirdClient;
    private BlockingQueue<String> msgQueue;

    public TwitterClient(Properties config, List<String> terms) {
        this.msgQueue = new LinkedBlockingQueue<String>(1000);

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(config.getProperty("app.twitter.consumerKey"),
                config.getProperty("app.twitter.consumerSecretKey"),
                config.getProperty("app.twitter.accessToken"),
                config.getProperty("app.twitter.secretAccessToken"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        this.hosebirdClient = builder.build();

    }

    public Client getHosebirdClient() {
        return hosebirdClient;
    }

    public BlockingQueue<String> getMsgQueue(){
        return msgQueue;
    }
}
