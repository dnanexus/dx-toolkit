#include "Thread.h"

#include <signal.h>
#include <unistd.h>
#include <string>

void Die(const char *msg)
{
  throw std::string(msg);
}

void Thread::Create(Thread &t, void *(*start_routine)(void*), void *data, size_t stack_size)
{
  if (pthread_attr_setstacksize(&t.attr, stack_size) != 0)
    Die("POSIX thread stack error");
  if (pthread_create(&t.thread, &t.attr, start_routine, data) != 0)
    Die("POSIX thread creation error");
}

void Thread::MicroSleep()
{
  usleep(15);
}

void Thread::IgnoreSignal(int signum)
{
  sigset_t s;
  sigemptyset(&s);
  sigaddset(&s, signum);
  pthread_sigmask(SIG_BLOCK, &s, NULL);
}

void Thread::Init()
{
  if (pthread_attr_init(&attr) != 0)
    Die("POSIX thread initialization error");
}

void Thread::Join()
{
  if (pthread_join(thread, NULL) != 0)
    Die("POSIX thread join error");
}

Lock::Lock()
{
  if (pthread_mutex_init(&mutex, NULL) != 0)
    Die("POSIX thread mutex initialization error");
}

Lock::~Lock()
{
  pthread_mutex_destroy(&mutex);
}

void Lock::Acquire()
{
  if (pthread_mutex_lock(&mutex) != 0)
    Die("POSIX thread mutex lock error");
}

void Lock::Release()
{
  if (pthread_mutex_unlock(&mutex) != 0)
    Die("POSIX thread mutex unlock error");
}

AutoLock::AutoLock(Lock &lock, bool acquire)
  : l(lock)
{
  if (acquire)
    l.Acquire();
  held = acquire;
}

AutoLock::~AutoLock()
{
  if (held)
    l.Release();
}

void AutoLock::Release()
{
  if (held)
  {
    l.Release();
    held = false;
  }
}

void AutoLock::Acquire()
{
  if (!held)
  {
    l.Acquire();
    held = true;
  }
}
