import redis.clients.jedis.Jedis

object ddd {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("192.168.186.102",6379)
    jedis.auth("123456")
    jedis.set("aa","sss")
  }

}
