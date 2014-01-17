#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <hiredis/hiredis.h>
#include <apr_pools.h>
#include <apr_hash.h>
#include <osip2/osip.h>

#ifndef __REDIS_OPERATING_H
#define __REDIS_OPERATING_H

typedef struct redis_reply_result
{
	int type;
	char *result;
}redis_reply_result_t;

typedef struct hash_string
{
	apr_pool_t *pool;
	char *string;
}hash_string_t;

redisContext *__redis_operating_connect(const char *ip, int port, struct timeval *tv);

int __redis_operating_execution(redisContext* gClient, char *strcmd, apr_pool_t *pool, redis_reply_result_t *reply_result);

osip_ring_t* __redis_operating_reader(apr_pool_t *pool, redisContext* gClient, char *strcmd);

int __redis_operating_execution_multi(apr_pool_t *pool, redisContext* gClient, osip_ring_t* ring);

apr_hash_t* __redis_operating_reader_multi(redisContext* gClient, apr_pool_t *pool, osip_ring_t* ring);

int redis_operating_hset(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, char *field, char *value);

int redis_operating_hmset(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, apr_hash_t *hash);

int redis_operating_hget(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, 
						 char *field, char **value);

int redis_operating_hmget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key,  osip_ring_t *key_ring, osip_ring_t **val_ring);

int redis_operating_sadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring);

int redis_operating_srem(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, char *key, osip_ring_t *ring);

int redis_operating_del(apr_pool_t *pool, const char *ip, int port, const struct timeval tv, osip_ring_t *key_ring);

int redis_operating_set(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *value, int ex_flag);

int redis_operating_mset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, apr_hash_t *hash);

int redis_operating_get(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char **value);

int redis_operating_mget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,osip_ring_t *key_ring, osip_ring_t **val_ring);

int redis_operating_keys(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t **val_ring);

int redis_operating_exists(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key);

#endif
