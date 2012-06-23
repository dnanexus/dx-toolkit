#include "bqueue.h"

void BlockingQueue::setCapacity(int capacity_) {
  capacity = capacity_;
}

void BlockingQueue::produce(Chunk * chunk) {
  {
    boost::unique_lock<boost::mutex> lock(mut);
    if (capacity != -1) {
      while (chunks.size() == capacity) {
        canProduce.wait(lock);
      }
    }
    chunks.push(chunk);
  }
  canConsume.notify_all();
}

Chunk * BlockingQueue::consume() {
  Chunk * chunk;
  {
    boost::unique_lock<boost::mutex> lock(mut);
    while (chunks.empty()) {
      canConsume.wait(lock);
    }
    chunk = chunks.front();
    chunks.pop();
  }
  canProduce.notify_all();
  return chunk;
}

size_t BlockingQueue::size() {
  return chunks.size();
}
