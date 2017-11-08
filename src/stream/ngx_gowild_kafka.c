//
//  ngx_gowild_kafka.c
//  qdreamer-proxy
//
//  Created by psyun on 2017/9/28.
//  Copyright © 2017年 gowild. All rights reserved.
//

#include "ngx_stream.h"
#include <stdlib.h>

typedef struct {
    char      *deviceId;
    char      *hook;
    char      *body;
} ngx_gowild_kafka_msg_t;

void add_line(char **res, int *line_offset, char *buf){
    res[*line_offset] = (char*)malloc(((int)strlen(buf)) * sizeof(char*));
    strcpy(res[*line_offset], buf);
    *line_offset += 1;
}

char*
ngx_strncpy(char *dest, const char *src, size_t n){
    int size = sizeof(char) * (n + 1);
    char *tmp = (char*)malloc(size);

    if(tmp){
        memset(tmp, '\0', size);
        memcpy(tmp, src, size - 1);
        memcpy(dest, tmp, size);

        free(tmp);
        return dest;

    }
    else{
        return NULL;
    }
}

ngx_gowild_kafka_msg_t*
ngx_request_line_value(char **req_line,int line_offset , ngx_log_t *log) {
    char *p;
    char *buf;
    int i = 0;
    int j = 0;
    int len;
    ngx_gowild_kafka_msg_t *gkm;
    gkm = (ngx_gowild_kafka_msg_t*)malloc(sizeof(ngx_gowild_kafka_msg_t));

    for(; i < line_offset; i++) {
        p = req_line[i];
        len = (int)strlen(p);
        buf = (char*)malloc(sizeof(char*) * len);
        j = 0;

        //analysis current response header line
        while(*p != '\0') {
            if(*p != ':') {
                p++;
                j++;
                continue;

            } else {
                //copy header name to buf
                ngx_strncpy(buf, req_line[i], j);

                if(!strcmp(buf, "device")){
                    gkm->deviceId = (char*)malloc(sizeof(char*) * (len - j - 1));
                    strcpy(gkm->deviceId, p + 1);
                } else if (!strcmp(buf, "hook")) {
                    gkm->hook = (char*)malloc(sizeof(char*) * (len - j - 1));
                    strcpy(gkm->hook, p + 1);
                }
                break;
            }
        }

        free(buf);
    }
    
    return gkm;
}

int32_t (*partitioner) (
    					const rd_kafka_topic_t *rkt,
    					const void *keydata,
    					size_t keylen,
    					int32_t partition_cnt,
    					void *rkt_opaque,
    					void *msg_opaque){
    return 0;
}

ngx_gowild_kafka_msg_t* 
ngx_gowild_msg_parse(char *msg, ngx_log_t *log){
    int offset = 0;
    int i = 0;
    char buf[512];
    int slash_count = 0;
    int line_offset = 0;
    ngx_gowild_kafka_msg_t *gkm;
    char **res;
    res = (char**)malloc(10 * sizeof(char*));

    do {
        if(slash_count == 2) {
            buf[offset] = msg[i];
            offset += 1;
        } else if(msg[i] == '\r' && msg[i + 1] == '\n') {
            i += 2;
            buf[offset] = '\0';
            add_line(res, &line_offset, buf);

            offset = 0;
            slash_count += 1;
            
            continue;

        } else {
            slash_count = 0;
            buf[offset] = msg[i];
            offset += 1;
        }

        i += 1;
    } while (msg[i] != '\0');

    gkm = ngx_request_line_value(res, line_offset, log);
    gkm->body = buf;

    return gkm;
}

char*
ngx_serialize_msg(ngx_gowild_kafka_msg_t *gkm) {
    int msg_len;
    char *msg_str;

    msg_len = strlen(gkm->hook) + strlen(gkm->deviceId) + strlen(gkm->body) + 2;
    msg_str = (char*)malloc(sizeof(char) * msg_len);
    strcpy(msg_str, gkm->hook);
    strcat(msg_str, gkm->deviceId);
    strcat(msg_str, " ");
    strcat(msg_str, gkm->body);

    return msg_str;
}

void
ngx_gowild_kafka_send_msg(ngx_log_t *log, ngx_buf_t *b) {
    char *msg;
    size_t len;
    ngx_gowild_kafka_msg_t *gkm;

    gkm = ngx_gowild_msg_parse((char*)b->pos, log);
    msg = ngx_serialize_msg(gkm);
    len = strlen(msg);
    ngx_log_info2(NGX_LOG_DEBUG_STREAM, log, 0, "ngx_gowild_msg,msg: %s, len: %d", msg, len);

    if(rd_kafka_produce(rkt,
                    RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY,
                    msg, len,
                    NULL, 0,
                    NULL) == -1){
                        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, log, 0,
                                "Failed send msg to kafka:%s",
                                rd_kafka_err2str(rd_kafka_last_error()));
    }
    free(gkm);
}
