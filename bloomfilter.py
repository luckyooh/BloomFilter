import math
import mmh3
import redis


class BloomFilter:
    BLOCK_SIZE = 512 * 1024 * 1024 * 8  # 定义每个块大小(最大512M)

    def __init__(self, capacity=1000000000, error_rate=0.0001, redis_client=None, key='BloomFilter'):
        self.capacity = capacity    # 期望过滤的元素数量
        self.error_rate = error_rate    # 允许的误判率
        self.redis_client = redis_client    # Redis 客户端
        self.key = key  # 存储在 Redis 中的键名前缀
        self.m = self._get_optimal_m(capacity, error_rate)  # 计算最优的位数组长度
        self.k = self._get_optimal_k(capacity, self.m)  # 计算最优的哈希函数个数
        self.mem = self._get_optimal_mem(self.m)    # 需要的总内存量（MB）
        self.block = self._get_optimal_block(self.m)    # 计算需要的块数

    @staticmethod
    def _get_optimal_m(capacity, error_rate):
        # 计算最优的位数组长度
        m = -(capacity * math.log(error_rate)) / (math.log(2) ** 2)
        return int(math.ceil(m))

    @staticmethod
    def _get_optimal_k(capacity, m):
        # 计算最优的哈希函数个数
        k = (m / capacity) * math.log(2)
        return int(math.ceil(k))

    @staticmethod
    def _get_optimal_mem(m):
        # 计算需要的内存量（MB）
        mem = m / 8 / 1024 / 1024
        return int(math.ceil(mem))

    def _get_optimal_block(self, m):
        # 计算需要的 Redis 块数量
        block = m / self.BLOCK_SIZE
        return int(math.ceil(block))

    def _get_block_key(self, position):
        # 获取位于特定位置的 Redis 键和位 的位置
        block_index = position // self.BLOCK_SIZE
        block_position = position % self.BLOCK_SIZE
        return f"{self.key}_{block_index}", block_position

    def add(self, item):
        # 添加元素到布隆过滤器
        pipeline = self.redis_client.pipeline()
        for seed in range(self.k):
            global_position = mmh3.hash64(item, seed)[0] % self.m
            block_key, block_position = self._get_block_key(global_position)
            pipeline.setbit(block_key, block_position, 1)
        pipeline.execute()

    def exists(self, item):
        # 检查元素是否可能存在于布隆过滤器中
        pipeline = self.redis_client.pipeline()
        for seed in range(self.k):
            global_position = mmh3.hash64(item, seed)[0] % self.m
            block_key, block_position = self._get_block_key(global_position)
            pipeline.getbit(block_key, block_position)
        results = pipeline.execute()
        return all(results)

    def __str__(self):
        return f"BloomFilter(m={self.m}, k={self.k}, mem={self.mem} , block={self.block}, capacity={self.capacity}, error_rate={self.error_rate})"


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, password='<password>')
    conn = redis.StrictRedis(connection_pool=pool)
    
    bf = BloomFilter(redis_client=conn)
    print(bf)
    bf.add("aaa")
    bf.add("bbb")
    print(bf.exists("aaa"))
    print(bf.exists("ccc"))
