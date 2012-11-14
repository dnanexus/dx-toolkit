#ifndef WINDOWS_BUILD
#include "SSLThreads.h"

#include <pthread.h>
#include <openssl/crypto.h>

/*
 * Setup code to use OpenSSL with pthreads.
 *
 * This code must be invoked by any program that uses OpenSSL, either
 * directly or indirectly, from within a pthread.
 */

void pthreads_locking_callback(int mode, int type, char *file, int line);
unsigned long pthreads_thread_id(void);

static pthread_mutex_t *lock_cs;
static long *lock_count;

void SSLThreadsSetup(void)
{
  int i;

  lock_cs = (pthread_mutex_t *) OPENSSL_malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t));
  lock_count = (long *) OPENSSL_malloc(CRYPTO_num_locks() * sizeof(long));
  for (i = 0; i < CRYPTO_num_locks(); i++)
  {
    lock_count[i] = 0;
    pthread_mutex_init(&(lock_cs[i]), NULL);
  }

//   fprintf(stderr, "thread_setup:");
  CRYPTO_set_id_callback((unsigned long (*)()) pthreads_thread_id);
//   fprintf(stderr, " id_callback");
  CRYPTO_set_locking_callback((void (*)(int, int, const char *, int)) pthreads_locking_callback);
//   fprintf(stderr, " locking_callback");
//   fprintf(stderr, "\n");
}

void SSLThreadsCleanup(void)
{
  int i;

  CRYPTO_set_locking_callback(NULL);
//   fprintf(stderr, "cleanup\n");
  for (i = 0; i < CRYPTO_num_locks(); i++)
  {
    pthread_mutex_destroy(&(lock_cs[i]));
//     fprintf(stderr, "%8ld:%s\n", lock_count[i], CRYPTO_get_lock_name(i));
  }
  OPENSSL_free(lock_cs);
  OPENSSL_free(lock_count);

//   fprintf(stderr, "done cleanup\n");
}

void pthreads_locking_callback(int mode, int type,
                               char *file __attribute__ ((unused)),
                               int line __attribute__ ((unused)))
{
  if (mode & CRYPTO_LOCK)
  {
    pthread_mutex_lock(&(lock_cs[type]));
    lock_count[type]++;
  }
  else
  {
    pthread_mutex_unlock(&(lock_cs[type]));
  }
}

unsigned long pthreads_thread_id(void)
{
  unsigned long ret;

  ret = (unsigned long) pthread_self();
  return (ret);
}
#endif
