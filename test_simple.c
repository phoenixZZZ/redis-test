#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <hiredis/hiredis.h>
#include "redis_operating.h"
#include <osip2/osip.h>
#include <apr_strings.h>
#include <apr_pools.h>
#include <unistd.h>
#include <apr_thread_pool.h>
#include <apr_time.h>

int main()
{
/*
	unsigned int j;
	redisContext *c;
	redisReply *reply;
	int size = 100000;

	clock_t start,finish;
	double jishi;
	char key[1000];
	char val[1000];
	char times[100];

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	c = redisConnectWithTimeout((char*)"127.0.0.1", 6379, timeout);
	if (c->err) {
		printf("Connection error: %s\n", c->errstr);
		exit(1);
	}
	start = clock();
	return 0;
*/

	int i = 0;int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	apr_pool_create(&pool, NULL);

	apr_time_t now = 0;
	now = apr_time_now();

	//测试HMSET/HMGET
	/*for(j=0;j<10000;j++)
	{
		apr_pool_create(&subpool, pool);
		hash = apr_hash_make(subpool);

		tmp1 = apr_psprintf(subpool, "test1:%d:%d", i, i+1);
		tmp2 = apr_psprintf(subpool, "%d", i+101);
		apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);

		tmp1 = apr_psprintf(subpool, "test2:%d:%d", i, i+1);
		tmp2 = apr_psprintf(subpool, "%d", i+101);
		apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);

		tmp1 = apr_psprintf(pool, "test_hmeset-%d-%d", j, i);
		redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash);

		apr_hash_clear(hash);
		apr_pool_destroy(subpool);
	}

	for(j=0;j<10000;j++)
	{
		apr_pool_create(&subpool, pool);
		osip_ring_create(subpool, &key_ring);

		tmp1 = apr_psprintf(subpool, "test1:%d:%d", i, i+1);
		osip_ring_add(key_ring, tmp1, -1);

		tmp1 = apr_psprintf(subpool, "test2:%d:%d", i, i+1);
		osip_ring_add(key_ring, tmp1, -1);

		tmp1 = apr_psprintf(pool, "test_hmeset-%d-%d", j, i);
		redis_operating_hmget(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, key_ring, &val_ring);

		apr_pool_destroy(subpool);
	}*/

	//测试mset
	/*hash_str = apr_hash_make(pool);
	osip_ring_create(pool, &string_key_ring);
	for(i = 0;i < 10000; i++)
	{
		tmp1 = apr_psprintf(pool, "1:%d:%d", i, i+1);
		tmp2 = apr_psprintf(pool, "%d", i+101);
		apr_hash_set(hash_str, tmp1, APR_HASH_KEY_STRING, tmp2);
		//osip_ring_add(string_key_ring, tmp1, -1);

		tmp1 = apr_psprintf(pool, "2:%d:%d", i, i+1);
		tmp2 = apr_psprintf(pool, "%d", i+101);
		apr_hash_set(hash_str, tmp1, APR_HASH_KEY_STRING, tmp2);
		//osip_ring_add(string_key_ring, tmp1, -1);
	}

	val_ring = NULL;
	redis_operating_mset(pool,(char*)"127.0.0.1", 6379, timeout, hash_str);*/

	//测试mget
	/*
	tmp1 = apr_psprintf(pool, "2:%d:%d", 1, 2);
	osip_ring_add(string_key_ring, tmp1, -1);
	tmp1 = apr_psprintf(pool, "1:%s:%d", "a1", 1);
	osip_ring_add(string_key_ring, tmp1, -1);
	tmp1 = apr_psprintf(pool, "2s:%d:%d", 4141, 4142);
	osip_ring_add(string_key_ring, tmp1, -1);

	redis_operating_mget(pool,(char*)"127.0.0.1", 6379, timeout, string_key_ring, &val_ring);
	tmp1 = apr_psprintf(pool, "%d:*", 2);
	redis_operating_keys(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, &val_ring);

	void *obj;
	osip_ring_iterator_t *it = NULL;
	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));
	int iCount = 0;

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(val_ring, it); obj; obj = osip_ring_get_next(it))
	{
		iCount++;
		apr_pool_create(&subpool2, pool);
	    printf("%d : %s\n", iCount, (char *)obj);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);*/

	//set/get, hset/hget测试
	/*tmp1 = apr_psprintf(pool, "1:%d:%d", i, i+1);
	tmp2 = apr_psprintf(pool, "%d", i+101);

	redis_operating_set(pool, (char*)"127.0.0.1", 6379, timeout, tmp1, tmp2, -1);
	redis_operating_get(pool, (char*)"127.0.0.1", 6379, timeout, tmp1, &tmp2);
	printf("GET:%s\n", tmp2);

	tmp2 = apr_psprintf(pool, "%d", i+101);
	redis_operating_hset(pool, (char*)"127.0.0.1", 6379, timeout, "hash_hey_1", tmp1, tmp2);
	redis_operating_hget(pool, (char*)"127.0.0.1", 6379, timeout, "hash_hey_1", tmp1, &tmp2);
	printf("HGET:%s\n", tmp2);*/

	//测试EXISTS
	//redis_operating_exists(pool,(char*)"127.0.0.1", 6379, timeout, "2:1:2");

	//SADD/SREM测试
	/*osip_ring_create(pool, &val_ring);
	for(i = 0;i < 10000; i++)
	{
		tmp2 = apr_psprintf(pool, "%d", i+101);
		osip_ring_add(val_ring, tmp2, -1);
	}

	redis_operating_sadd(pool, (char*)"127.0.0.1", 6379, timeout, "set_test1", val_ring);
	redis_operating_sadd(pool, (char*)"127.0.0.1", 6379, timeout, "set_test2", val_ring);

	osip_ring_create(pool, &key_ring);osip_ring_add(key_ring, "set_test1", -1);osip_ring_add(key_ring, "set_test2", -1);
	redis_operating_del(pool, (char*)"127.0.0.1", 6379, timeout, key_ring);*/

	test_thread_pool();
	return;
}

void test_fork()
{
	pid_t fpid;

	int i = 0;int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	apr_pool_create(&pool, NULL);

	fpid=fork();
	if (fpid < 0)
		printf("error in fork!");
	else if (fpid == 0) {
		for(j=0;j<10000;j++)
		{
			apr_pool_create(&subpool, pool);
			hash = apr_hash_make(subpool);
			tmp1 = apr_psprintf(subpool, "c-test1:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(subpool, "c-test2:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(pool, "c-test_hmeset-%d-%d", j, i);
			redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash, NULL);
			apr_hash_clear(hash);
			apr_pool_destroy(subpool);
		}
	}
	else {
		for(j=0;j<10000;j++)
		{
			apr_pool_create(&subpool, pool);
			hash = apr_hash_make(subpool);
			tmp1 = apr_psprintf(subpool, "p-test1:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(subpool, "p-test2:%d:%d", i, i+1);
			tmp2 = apr_psprintf(subpool, "%d", i+101);
			apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
			tmp1 = apr_psprintf(pool, "p-test_hmeset-%d-%d", j, i);
			redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash, NULL);
			apr_hash_clear(hash);
			apr_pool_destroy(subpool);
		}
	}
	return 0;
}

apr_pool_t *__pool = NULL;

static void *task_handle(apr_thread_t *me, void *param)
{
	int i = 0;int j = (int)param;
	redisContext *c;
	redisReply *reply;
	
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;
	apr_thread_pool_t *thread_pool = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_create(&subpool, __pool);

	/*hash = apr_hash_make(subpool);
	tmp1 = apr_psprintf(subpool, "p%d-test1:%d:%d", j, i, i+1);
	tmp2 = apr_psprintf(subpool, "%d", i+101);
	apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
	tmp1 = apr_psprintf(subpool, "p%d-test2:%d:%d", j, i, i+1);
	tmp2 = apr_psprintf(subpool, "%d", i+101);
	apr_hash_set(hash, tmp1, APR_HASH_KEY_STRING, tmp2);
	tmp1 = apr_psprintf(pool, "p-test_hmeset-%d-%d", j, i);
	redis_operating_hmset(pool,(char*)"127.0.0.1", 6379, timeout, tmp1, hash);
	apr_hash_clear(hash);
	apr_pool_destroy(subpool);*/

	tmp1 = apr_psprintf(subpool, "1:%d:%d", 1, 2);
	tmp2 = apr_psprintf(subpool, "new-%d", j);
	redis_operating_get(subpool, (char*)"127.0.0.1", 6379, timeout, tmp1, &tmp2);
	redis_operating_set(subpool, (char*)"127.0.0.1", 6379, timeout, tmp1, tmp2, -1);
	printf("GET:%s\n", tmp2);
	apr_pool_destroy(subpool);
}

void test_thread_pool()
{
	pid_t fpid;
	int j = 0;
	redisContext *c;
	redisReply *reply;
	apr_pool_t *__pool = NULL;
	apr_pool_t *subpool = NULL;
	apr_hash_t *hash = NULL;
	apr_hash_t *hash_str = NULL;
	char *tmp1 = NULL;
	char *tmp2 = NULL;
	osip_ring_t *key_ring = NULL;
	osip_ring_t *val_ring = NULL;
	osip_ring_t *string_key_ring = NULL;
	apr_thread_pool_t *thread_pool = NULL;

	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	apr_pool_initialize();
	apr_pool_create(&__pool, NULL);
	apr_thread_pool_create(&thread_pool, 1000, 1000, __pool);
	for(j=0;j<10000;j++)
	{
		apr_thread_pool_push(thread_pool, task_handle, j,
							 APR_THREAD_TASK_PRIORITY_NORMAL, NULL);
	}
	sleep(10);
}

void test_proxy_lock()
{
	
}
