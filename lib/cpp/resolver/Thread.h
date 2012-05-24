#ifndef RESOLVER_THREAD_H
#define RESOLVER_THREAD_H

#include <pthread.h>

class Thread
{
  public:

  static void Create(Thread &t, void *(*start_routine)(void*), void *data, size_t stack_size = 33554432);
  static void MicroSleep();
  static void IgnoreSignal(int signum);

  Thread() { Init(); }

  void Join();

  pthread_attr_t attr;
  pthread_t thread;
  int id;

  private:

  void Init();
};

class Lock
{
  public:

  Lock();
  ~Lock();

  void Acquire();
  void Release();

  private:

  pthread_mutex_t mutex;

};

class AutoLock
{
  public:

  bool held;

  AutoLock(Lock &lock, bool acquire = true);
  ~AutoLock();
  void Release();
  void Acquire();

  private:

  Lock &l;
};

#define spin(lock, cond) do { while(cond) { (lock).Release(); Thread::MicroSleep(); (lock).Acquire(); } } while(false)

#endif
