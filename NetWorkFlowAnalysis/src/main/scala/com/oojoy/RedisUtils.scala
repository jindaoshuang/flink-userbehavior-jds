package com.oojoy

import redis.clients.jedis._

object RedisUtils extends Serializable {

  var readJedis: Jedis = _
  var writeJedis: Jedis = _


//  //验证redis连接
//  def checkAlive: Jedis = {
//    //判断是否连接可用
//    if (!isConnected(readJedis))
//      readJedis = reConnect(readJedis)
//    readJedis
//    if (!isConnected(writeJedis))
//      writeJedis = reConnect(writeJedis)
//    writeJedis
//  }

  //查看连接情况
  def isConnected(jedis: Jedis) = jedis != null && jedis.isConnected


  def getConn(redisHost: String, redisPort: String, redispassword: String): Jedis = {
    val jedis = new Jedis(redisHost, redisPort.toInt)
    jedis.connect()
    if (redispassword.length > 0) {
      jedis.auth(redispassword)
    }
    jedis
  }
//
//  //重新连接
//  def reConnect(jedis: Jedis): Jedis = {
//    println("reconnecting ...")
//    disConnect(jedis)
//    getConn(GlobalConfigUtils.redisHost, GlobalConfigUtils.redisPort, GlobalConfigUtils.redisPassword)
//  }

  //释放连接
  def disConnect(jedis: Jedis) = {
    if (jedis != null && jedis.isConnected()) {
      jedis.close
    }
  }

}
