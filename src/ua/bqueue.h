#ifndef UA_BQUEUE_H
#define UA_BQUEUE_H

#include <queue>
#include <boost/thread.hpp>

/*
 * A synchronized, blocking queue of chunks. This provides a way for
 * chunks to be passed between worker threads.
 *
 * The 'produce' operation is used to insert a chunk into the
 * queue. This operation blocks if the capacity of the queue has been
 * reached.
 *
 * The 'consume' operation is used to obtain a remove a chunk from the
 * queue, returning it to the consumer. This operation blocks if there
 * are no chunks in the queue.
 */

class Chunk;  // forward declaration, so we don't need to include chunk.h

class BlockingQueue {
public:

  BlockingQueue(int capacity_ = -1)
    : capacity(capacity_)
  {
  }

  ~BlockingQueue()
  {
  }

  void setCapacity(int capacity_);
  void produce(Chunk * chunk);
  Chunk * consume();

  size_t size();

private:

  /* The capacity of the queue, or -1 if the capacity is unbounded. */
  int capacity;

  /* The underlying queue. */
  std::queue<Chunk *> chunks;

  boost::mutex mut;
  boost::condition_variable canProduce;
  boost::condition_variable canConsume;
};

#endif
