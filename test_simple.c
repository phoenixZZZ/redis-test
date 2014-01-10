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
	hash_str = apr_hash_make(pool);
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
	redis_operating_mset(pool,(char*)"127.0.0.1", 6379, timeout, hash_str);

	//测试EXISTS
	redis_operating_exists(pool,(char*)"127.0.0.1", 6379, timeout, "2:1:2");

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

}
