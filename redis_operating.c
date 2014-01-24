#include "redis_operating.h"
#include <apr_pools.h>
#include <apr_strings.h>

redisContext *__redis_operating_connect(const char *ip, int port, struct timeval *tv)
{
	int ret = -1;
    redisContext *gClient = NULL;

	if(NULL == ip)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(tv)
		gClient = redisConnectWithTimeout(ip, port, *tv);
	else
		gClient = redisConnect(ip, port);

	if ((NULL == gClient)||(gClient->err))
	{
    	printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
	}
	ret = 0;

END:
	if(ret != 0 && NULL != gClient)
	{
		redisFree(gClient);
		gClient = NULL;
	}
	return gClient;
}

int __redis_operating_execution(redisContext* gClient, char *strcmd, apr_pool_t *pool, redis_reply_result_t *reply_result)
{
	int ret = -1;
	redisReply* reply = NULL;

	if(NULL == gClient || NULL == strcmd)
	{
		printf("Error.redis operating execution failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
	}

	reply = (redisReply*)redisCommand(gClient, strcmd);
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }

	if(REDIS_REPLY_ERROR == reply->type)
	{
		ret = 0;
		if(!pool && !reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "error");
			reply_result->type = REDIS_REPLY_ERROR;
		}
	}
	else if(REDIS_REPLY_INTEGER == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_psprintf(pool, "%ld", reply->integer);
			reply_result->type = REDIS_REPLY_INTEGER;
		}
	}
	else if(REDIS_REPLY_NIL == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, "nil");
			reply_result->type = REDIS_REPLY_NIL;
		}
	}
	else if(REDIS_REPLY_STATUS == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STATUS;
		}
	}
	else if(REDIS_REPLY_STRING == reply->type)
	{
		ret = 0;
		if(NULL != pool && NULL != reply_result) 
		{
			reply_result->result = apr_pstrdup(pool, reply->str);
			reply_result->type = REDIS_REPLY_STRING;
		}
	}
	else
	{
		ret = -1;
	}

END:
    if (NULL != reply)
    {
        freeReplyObject(reply);
    }
	return ret;
}

osip_ring_t* __redis_operating_reader(apr_pool_t *pool, redisContext* gClient, char *strcmd)
{
	redisReply* reply = NULL;
	osip_ring_t *ret_ring = NULL;
	int ret = -1;
	char *add_str = NULL;

	reply = (redisReply*)redisCommand(gClient, strcmd);
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	else
	{
		if(0 != osip_ring_create(pool, &ret_ring))
		{
			printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
		}
		if (reply->type == REDIS_REPLY_ARRAY) 
		{
			int j = 0;
			for (j = 0; j < reply->elements; j++) {
				//osip_ring_add(ret_ring, reply->element[j]->str, -1);
				if(REDIS_REPLY_ERROR == reply->element[j]->type)
				{
					ret = 0;
					osip_ring_add(ret_ring, "error", -1);
				}
				else if(REDIS_REPLY_INTEGER == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%ld", reply->element[j]->integer);
					osip_ring_add(ret_ring, add_str, -1);
				}
				else if(REDIS_REPLY_NIL == reply->element[j]->type)
				{
					ret = 0;
					osip_ring_add(ret_ring, "nil", -1);
				}
				else if(REDIS_REPLY_STATUS == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%s", reply->element[j]->str);
				    osip_ring_add(ret_ring, add_str, -1);
				}
				else if(REDIS_REPLY_STRING == reply->element[j]->type)
				{
					ret = 0;add_str = apr_psprintf(pool, "%s", reply->element[j]->str);
				    osip_ring_add(ret_ring, add_str, -1);
				}
				else
				{
					ret = -1;
					goto END;
				}
			}
		}
		else
		{
			/*if (reply->type == REDIS_REPLY_STRING)
			{
hk				osip_ring_add(ret_ring, reply->str, -1);
			}
			else if (reply->type == REDIS_REPLY_INTEGER)
			{
				osip_ring_add(ret_ring, reply->integer, -1);
			}
			else
			{*/
			printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
			//}
		}
	}
	ret = 0;

END:
    if (NULL != reply)
    {
        freeReplyObject(reply);
    }
	if (ret != 0 && ret_ring != NULL)
	{
		void *obj = NULL;
		osip_ring_iterator_t *it = NULL;

		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(ret_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
		return NULL;
	}
	return ret_ring;
}

int __redis_operating_execution_multi(apr_pool_t *pool, redisContext* gClient, osip_ring_t* ring)
{
	int ret = -1;
	redisReply* reply = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	char *strcmd = NULL;

	if(NULL == gClient)
	{
		printf("redis operating executionargs by multi!");
		ret = -1;
		goto END;
	}

	reply = (redisReply*)redisCommand(gClient, "MULTI");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	freeReplyObject(reply);reply = NULL;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		strcmd = (char *)obj;
		if(__redis_operating_execution(gClient, strcmd, NULL, NULL) != 0)
		{
			printf("Info.redis command exec failed:%s.[%s:%d]\n", strcmd, __FILE__, __LINE__);
			ret = -1;
			reply = (redisReply*)redisCommand(gClient, "DISCARD");
			goto END;
		}
	}
	osip_ring_destroy_iterator(it);

	reply = (redisReply*)redisCommand(gClient, "EXEC");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }

END:
	if(!reply) freeReplyObject(reply);
	return ret;
}

apr_hash_t* __redis_operating_reader_multi(redisContext* gClient, apr_pool_t *pool, osip_ring_t* ring)
{
	int ret = -1;
	apr_hash_t *hash = NULL;
	osip_ring_t *tmp_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	redisReply* reply = NULL;
	void *obj = NULL;
	char *strcmd = NULL;

	if(ring == NULL)
	{
		printf("redis operating executionargs by multi!");
		ret = -1;
		goto END;
	}

	hash = apr_hash_make(pool);

	reply = (redisReply*)redisCommand(gClient, "MULTI");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	freeReplyObject(reply);reply = NULL;

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		strcmd = (char *)obj;
		tmp_ring = __redis_operating_reader(pool, gClient, strcmd);
		if(tmp_ring == NULL)
		{
			printf("Info.redis command exec failed:%s.[%s:%d]\n", strcmd, __FILE__, __LINE__);
			ret = -1;
			goto END;
		}
		apr_hash_set(hash, strcmd, APR_HASH_KEY_STRING, tmp_ring);
	}
	osip_ring_destroy_iterator(it);

	reply = (redisReply*)redisCommand(gClient, "EXEC");
    if (NULL == reply) 
    {
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
        ret = -1;
        goto END;
    }
	ret = 0;

END:
	if(!reply) freeReplyObject(reply);
	if(ret != 0 && hash != NULL)
	{
		apr_hash_clear(hash);
		return NULL;
	}
	return hash;
}

int redis_operating_hset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *field, char *value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redisContext* gClient = NULL;

	if(NULL == key || NULL == pool || NULL == field || NULL == value)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "HSET %s %s %s", key, field, value);
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_hmset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, apr_hash_t *hash, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
    char *hash_val = NULL;
	char *hash_key = NULL;
	redis_reply_result_t *reply_result = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HMSET %s ", key);
	if(NULL == str_val)
	{
		for (hi = apr_hash_first(pool, hash); hi; hi = apr_hash_next(hi)) 
		{
			apr_hash_this(hi, &hash_key, NULL, &hash_val);
			if (hash_val && hash_key)
			{
				apr_pool_create(&subpool2, pool);
				strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)hash_key, " ", (char *)hash_val, NULL);
				apr_pool_destroy(strcmd->pool);
				strcmd->pool = subpool2;
			}
			else
			{
				printf("Info.Connect to redis failed.[%s:%d]\n", __FILE__, __LINE__);
				ret = -1;
				goto END;
			}
		}
	}
	else
	{
		strcmd->string = apr_pstrcat(strcmd->pool, strcmd->string, " ", str_val, " ", NULL);
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_hget(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, 
						 char *field, char **value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "HGET %s %s", key, field);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*value = apr_pstrdup(pool, reply_result->result);
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_hmget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key,  osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HMGET %s", key);
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}


int redis_operating_hgetall(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv,char *key, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "HGETALL %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_set(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char *value, int ex_flag)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool || NULL == value)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	if(0 == ex_flag)
		strcmd = apr_psprintf(pool, "SET %s %s NX", key, value);
	else if(1 == ex_flag)
		strcmd = apr_psprintf(pool, "SET %s %s XX", key, value);
	else
		strcmd = apr_psprintf(pool, "SET %s %s", key, value);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_mset(apr_pool_t *pool, const char *ip, int port, struct timeval tv, apr_hash_t *hash)
{
	//char *strcmd = NULL;
	int ret = -1;
    char *hash_val = NULL;
	char *hash_key = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t *it = NULL;
	apr_hash_index_t *hi = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_pstrdup(subpool1, "MSET ");
    for (hi = apr_hash_first(pool, hash); hi; hi = apr_hash_next(hi)) 
    {
        apr_hash_this(hi, &hash_key, NULL, &hash_val);
        if (hash_val && hash_key)
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)hash_key, " ", (char *)hash_val, NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		else
		{
			printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
			ret = -1;
			goto END;
		}
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_get(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, char **value)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "GET %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n", __FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*value = apr_pstrdup(pool, reply_result->result);
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;

}

int redis_operating_mget(apr_pool_t *pool, const char *ip, int port, 
						  struct timeval tv, osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "MGET ");
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		//strcmd = apr_pstrcat(pool, strcmd, " ", (char *)obj, NULL);
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_keys(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t **val_ring)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	osip_ring_t *ret_ring = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "KEYS %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	ret_ring = __redis_operating_reader(pool, gClient, strcmd);
	if(NULL == ret_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	if(ret != 0) *val_ring = NULL;
	*val_ring = ret_ring;
	return ret;
}

int redis_operating_exists(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "EXISTS %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(3 != reply_result->type || 0 != strcasecmp(reply_result->result, "1"))
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

/*int redis_operating_hdel(const char *ip, int port, const struct timeval tv, char *strcmd);

  int redis_operating_hgetall_hvals(const char *ip, int port, const struct timeval tv, char *strcmd);*/

int redis_operating_sadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SADD %s ", key);

	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", str_val, " ", NULL);
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_srem(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t * it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "SREM %s ", key);

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_type(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "TYPE %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(1 != reply_result->type)
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}


int redis_operating_del(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key)
{
	char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;

	if(NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd = apr_psprintf(pool, "DEL %s", key);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd, pool, reply_result);
	if(0 != ret)
	{
		ret = -1;
		goto END;
	}

	if(1 != reply_result->type)
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_sinter(apr_pool_t *pool, const char *ip, int port, 
						   struct timeval tv, osip_ring_t *key_ring, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key_ring || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "SINTER ");
	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(key_ring, it); obj; obj = osip_ring_get_next(it))
	{
		//strcmd = apr_pstrcat(pool, strcmd, " ", (char *)obj, NULL);
		apr_pool_create(&subpool2, pool);
		if(redis_operating_type(pool, ip, port, tv, (char *)obj) != -1)
			strcmd->string = apr_pstrcat(subpool2, strcmd->string, " ", (char *)obj, NULL);
		else
		{
			printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
			apr_pool_destroy(subpool2);
			ret = -1;
			goto END;
		}
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	if(strcmd) apr_pool_destroy(strcmd->pool);
	return ret;
}

int redis_operating_zadd(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring, char *str_val)
{
	//char *strcmd = NULL;
	int ret = -1;
	osip_ring_iterator_t *it = NULL;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "ZADD %s ", key);

	if(NULL == str_val)
	{
		osip_ring_create_iterator(pool, &it);
		for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
		{
			apr_pool_create(&subpool2, pool);
			strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
			apr_pool_destroy(strcmd->pool);
			strcmd->pool = subpool2;
		}
		osip_ring_destroy_iterator(it);
	}
	else
	{
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", str_val, " ", NULL);
	}

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	

	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_zrem(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, osip_ring_t *ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	void *obj = NULL;
	osip_ring_iterator_t * it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == ring || NULL == key || NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "ZREM %s ", key);

	osip_ring_create_iterator(pool, &it);
	for(obj = osip_ring_get_first(ring, it); obj; obj = osip_ring_get_next(it))
	{
		apr_pool_create(&subpool2, pool);
		strcmd->string = apr_pstrcat(pool, strcmd->string, " ", (char *)obj, " ", NULL);
		apr_pool_destroy(strcmd->pool);
		strcmd->pool = subpool2;
	}
	osip_ring_destroy_iterator(it);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
		ret = 0;
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_zrangbyscore(apr_pool_t *pool, const char *ip, int port, 
						 struct timeval tv, char *key, char *min, char *max, osip_ring_t **val_ring)
{
	//char *strcmd = NULL;
	int ret = -1;
	apr_hash_index_t *hi = NULL;
	void *val = NULL;
	osip_ring_iterator_t *it = NULL;
	void *obj = NULL;
	osip_ring_t *tmp_ring = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == key || NULL == pool || NULL == min || NULL == max)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(subpool1, "ZRANGEBYSCORE %s %s %s", key, min, max);

	redisContext* gClient = NULL;
	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	tmp_ring = __redis_operating_reader(pool, gClient, strcmd->string);
	if(NULL == tmp_ring)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	*val_ring = tmp_ring;
	ret = 0;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

int redis_operating_incr(apr_pool_t *pool, const char *ip, int port, struct timeval tv, char *key, int increment)
{
	//char *strcmd = NULL;
	int ret = -1;
	redis_reply_result_t *reply_result = NULL;
	redisContext* gClient = NULL;
	void *obj = NULL;
	osip_ring_iterator_t *it = NULL;

	apr_pool_t *subpool1 = NULL;
	apr_pool_t *subpool2 = NULL;
	hash_string_t *strcmd = apr_pcalloc(pool, sizeof(hash_string_t));

	apr_pool_create(&subpool1, pool);
	strcmd->pool = subpool1;

	if(NULL == pool)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	strcmd->string = apr_psprintf(pool, "INCRBY %s %d", key, increment);

	gClient = __redis_operating_connect(ip, port, &tv);
	if(NULL == gClient)
	{
		printf("Info.Connect to redis failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}
	
	reply_result = apr_pcalloc(pool, sizeof(redis_reply_result_t));
	ret = __redis_operating_execution(gClient, strcmd->string, pool, reply_result);
	if(ret != 0)
	{
		printf("Info.redis command exec failed.[%s:%d]\n",__FILE__, __LINE__);
		ret = -1;
		goto END;
	}

	if(reply_result && 3 == reply_result->type)
	{
		int itmp = atoi(reply_result->result);
		if(itmp > 0) ret = itmp;
		else ret = -1;
	}
	else
		ret = -1;

END:
    if (NULL != gClient)
    {
        redisFree(gClient);
	}
	return ret;
}

//business-common layer

char *__strIP = "127.0.0.1";
int __port = 6379;
struct timeval __timeout = { 1, 500000 }; // 1.5 seconds

int __redis_update_class_id(apr_pool_t *pool, char *type)
{
	int ret = -1;
	if(type) goto END;
	char *tmp = apr_psprintf(pool, "%s:id", type);
	if(redis_operating_incr(pool, __strIP, __port, __timeout, tmp, 1) != 0)
	{
		ret = -1;
		goto END;
	}
	ret = 0;

END:
	return ret;
}

osip_ring_t* redis_get_class_id(apr_pool_t *pool, char *type, char *member, ...)
{
	va_list arg_ptr;
	int va_count = 0;

	char *tmp = NULL;
	char *arg_set = NULL;

	osip_ring_t *arg_ring = NULL;
	osip_ring_t *ret_ring = NULL;

	if(0 != osip_ring_create(pool, &arg_ring))
	{
		printf("Error.Add RedisCommand failed.[%s:%d]\n",__FILE__, __LINE__);
		ret_ring = NULL;
		goto END;
	}

	//偶数集合为对应的name，奇数集合为对应的value
	va_start(arg_ptr, member);
	do
	{
		tmp = va_arg(arg_ptr, char *);
		if(tmp && 0 == va_count%2)
		{
			arg_set = apr_pstrdup(pool, tmp);
		}
		else
		{
			arg_set = apr_pstrcat(pool, arg_set, tmp, NULL);
			osip_ring_add(arg_ring, arg_set, -1);
			arg_set = NULL;
		}
	}while(tmp != NULL);
	va_end(arg_ptr);

	if(redis_operating_sinter(pool, __strIP, __port, __timeout, arg_ring, &ret_ring) != 0)
	{
		ret_ring = NULL;
		goto END;
	}

END:
	if (arg_ring != NULL)
	{
		void *obj = NULL;
		osip_ring_iterator_t *it = NULL;

		osip_ring_create_iterator(pool, &it);
	    for(obj = osip_ring_get_first(arg_ring, it); obj; obj = osip_ring_get_next(it))
		{
			osip_ring_iterator_remove(it);
		}
	    osip_ring_destroy_iterator(it);
	}
	return ret_ring;
}

int __redis_set_class_num(apr_pool_t *pool, char *type, int id, void *value, func_call_class func)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == type || NULL == value || NULL == func || id <=0)
	{
		ret = -1;
		goto END;
	}

	val = func(pool, value);
	if(val)
	{
		key = apr_psprintf(pool, "%s:%d", type, id);
		if(redis_operating_hmset(pool, __strIP, __port, __timeout, key, NULL, val) != -1)
		{
			ret = 0;
			goto END;
		}
	}

END:
	return ret;
}

int __redis_set_class_all(apr_pool_t *pool, char *type, int id)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == type || id <=0)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:all", type);
	val = apr_psprintf(pool, "%s:%d", type, id);
	redis_operating_sadd(pool, __strIP, __port, __timeout, key, NULL, val);

	ret = 0;
END:
	return ret;
}


int __redis_set_class_timerheap(apr_pool_t *pool, char *type, char *val, apr_time_t timer)
{
	int ret = -1;
	char *key = NULL;
	char *tmp = NULL;

	if(NULL == pool || NULL == type || NULL == val)
	{
		ret = -1;
		goto END;
	}

	if(0 == strcasecmp(key, "yaproxy_lock_t"))
	{
		key = "yaproxy_lock_timerheap";
	}
	else
	{
		
	}

	tmp = apr_psprintf(pool, "%ld %s", timer, val);
	redis_operating_zadd(pool, __strIP, __port, __timeout, key, NULL, tmp);
	ret = 0;

END:
	return ret;
}

int redis_get_class_timerheap(apr_pool_t *pool, char *type, apr_time_t timer, osip_ring_t **result)
{
	int ret = -1;
	char *key = NULL;
	char *sorce = NULL;

	if(NULL == pool || NULL == type)
	{
		ret = -1;
		goto END;
	}

	if(0 == strcasecmp(key, "yaproxy_lock_t"))
	{
		key = "yaproxy_lock_timerheap";
	}
	else
	{
		
	}

	sorce = apr_psprintf(pool, "%ld", timer);
	if(redis_operating_zrangbyscore(pool, __strIP, __port, __timeout, key, "-inf", sorce, result) != 0)
	{
		*result = NULL;
		ret = -1;
		goto END;
	}

	if(result && redis_operating_zrem(pool, __strIP, __port, __timeout, key, result) != 0)
	{
		*result = NULL;
		ret = -1;
		goto END;
	}

	ret = 0;
END:
	return ret;
}

int __redis_set_class_memberset(apr_pool_t *pool, char *type, char *member_name, char *member_val, int id)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == type || id <=0)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%s:%s", type, member_name, member_val);
	val = apr_psprintf(pool, "%d", id);
	redis_operating_sadd(pool, __strIP, __port, __timeout, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}

int __redis_set_class_indices(apr_pool_t *pool, char *type, char *member_name, char *member_val, int id)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == type || id <=0)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_indices", type, id);
	val = apr_psprintf(pool, "%s:%s:%s", type, member_name, member_val);
	redis_operating_sadd(pool, __strIP, __port, __timeout, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}

int __redis_set_class_zindices(apr_pool_t *pool, char *type, char *member_name, char *member_val, int id)
{
	int ret = -1;
	char *key = NULL;
	char *val = NULL;

	if(NULL == pool || NULL == type || id <=0)
	{
		ret = -1;
		goto END;
	}

	key = apr_psprintf(pool, "%s:%d:_zindices", type, id);
	val = apr_psprintf(pool, "%s:%s:%s", type, member_name, member_val);
	redis_operating_sadd(pool, __strIP, __port, __timeout, key, NULL, val);
	
	ret = 0;
END:
	return ret;
}

int redis_del_objects_bymember(apr_pool_t *pool, char *type, char *member, ...)
{
	
}

int redis_del_single_object_byid(apr_pool_t *pool, char *type, int id)
{
	int ret = -1;
	osip_ring_t *val_ring = NULL;
	osip_ring_iterator_t *it = NULL;
	char *key = NULL;

	key = apr_psprintf(pool, "%s:%d", type, id);
	if(0 != __redis_del_single_object_dictset(pool, type, id, key))
	{
		
	}

END:
	return ret;
}

int __redis_del_single_object_dictset(apr_pool_t *pool, char *type, int id, char *key)
{
	
}

int __redis_del_single_object_memberset(apr_pool_t *pool, char *key)
{
	
}

int redis_update_object_byid(apr_pool_t *pool, char *type, int id)
{
	
}

//proxy_lock moudle
char* func_call_yaproxy_lock(apr_pool_t *pool, void *obj)
{
	char *result = NULL;
	char *tmp = NULL;
	yaproxy_lock_t *lock_obj = (yaproxy_lock_t *)obj;

	if(NULL == lock_obj)
	{
		result = NULL;
		goto END;
	}

	result = apr_pstrdup(pool, "");
	if(lock_obj->clientID)
	{
		tmp = apr_psprintf(pool, "%s", lock_obj->clientID);
		result = apr_pstrcat(pool, result, tmp, NULL);
	}
	if(lock_obj->ipcGbID)
	{
		tmp = apr_psprintf(pool, "%s", lock_obj->ipcGbID);
		result = apr_pstrcat(pool, result, tmp, NULL);
	}
	if(lock_obj->level)
	{
		tmp = apr_psprintf(pool, "%d", lock_obj->level);
		result = apr_pstrcat(pool, result, tmp, NULL);
	}
	if(lock_obj->timer)
	{
		tmp = apr_psprintf(pool, "%ld", lock_obj->timer);
		result = apr_pstrcat(pool, result, tmp, NULL);
	}

END:
	return result;
}
