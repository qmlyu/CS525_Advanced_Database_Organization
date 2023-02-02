# Compile program

```{bash}
cd assign2
make assign2_1
make assign2_2
```

# Test cases

### testCreateOpenClose

```{bash}
./assign2_1

./assign2_2
```

## FIFO
使用双向链表实现先进先出栈

## LRU
在缓存中维护一个双向链表，该链表奖缓存中的数据块按访问时间从新到旧排列起来

## LRU_K
在缓存中维护一个双向链表，该链表奖缓存中的数据块按访问时间从新到旧排列起来,当读取节点k次后把节点放到头节点