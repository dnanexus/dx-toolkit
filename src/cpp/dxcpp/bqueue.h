// Copyright (C) 2013-2014 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#ifndef UA_BQUEUE_H
#define UA_BQUEUE_H

#include <queue>
#include <boost/thread.hpp>

namespace dx {
  /**
   * A synchronized, blocking queue of chunks. This provides a way for chunks to be passed between
   * worker threads.
   *
   * The 'produce' operation is used to insert a chunk into the queue. This operation blocks if the
   * capacity of the queue has been reached.
   *
   * The 'consume' operation is used to obtain a remove a chunk from the queue, returning it to the
   * consumer. This operation blocks if there are no chunks in the queue.
   */
  template<typename T>
  class BlockingQueue {
  public:

    BlockingQueue(int capacity_ = -1) : capacity(capacity_) {
    }

    void setCapacity(int capacity_);
    int getCapacity() const;
    void produce(T chunk);
    T consume();

    size_t size() const;
    bool empty() const;
    
  private:

    /* The capacity of the queue, or -1 if the capacity is unbounded. */
    int capacity;

    /* The underlying queue. */
    std::queue<T> chunks;
    
    boost::mutex mut;
    boost::condition_variable canProduce;
    boost::condition_variable canConsume;
  };

  template<typename T> void BlockingQueue<T>::setCapacity(int capacity_) {
    capacity = capacity_;
  }

  template<typename T> int BlockingQueue<T>::getCapacity() const {
    return capacity;
  }

  template<typename T> void BlockingQueue<T>::produce(T chunk) {
    {
      boost::unique_lock<boost::mutex> lock(mut);
      if (capacity != -1) {
        while (chunks.size() == (size_t) capacity) {
          canProduce.wait(lock);
        }
      }
      chunks.push(chunk);
    }
    canConsume.notify_all();
  }

  template<typename T> T BlockingQueue<T>::consume() {
    T chunk;
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

  template<typename T> size_t BlockingQueue<T>::size() const {
    return chunks.size();
  }

  template<typename T> bool BlockingQueue<T>::empty() const {
    return chunks.empty();
  }
}

#endif
