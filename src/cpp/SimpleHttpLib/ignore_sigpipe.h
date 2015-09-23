// Copyright (C) 2013-2015 DNAnexus, Inc.
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

// This file provides functions for ignoring and restoring SIGPIPE.
// Inspired from this patch for "curl": http://curl.haxx.se/mail/lib-2013-03/att-0122/0001-SIGPIPE-ignore-it-while-inside-the-library.patch
// See JIRA ticket [PTFM-7251] to understand when we need to ignore SIGPIPE, also see: http://curl.haxx.se/mail/lib-2013-03/0122.html

#ifndef DX_IGNORE_SIGPIPE_H
#define DX_IGNORE_SIGPIPE_H

#if !WINDOWS_BUILD
#include <csignal>

struct sigpipe {
  struct sigaction action;
};

#define SIGPIPE_VARIABLE(x) struct sigpipe x

/*
 * sigpipe_ignore() makes sure we ignore SIGPIPE while running libcurl
 * internals, and then sigpipe_restore() will restore the situation when we
 * return from libcurl again.
 *
 * sa_handler: either a pointer to the function expected to be called on SIGPIPE
 *             (usually for printing info in logs), or NULL if we want to silently
 *             ignore SIGPIPE
 */
static void sigpipe_ignore(struct sigpipe *pipe, void (*handler_func) (int) = NULL) {
  struct sigaction action;
  /* first, extract the existing situation */
  sigaction(SIGPIPE, NULL, &pipe->action);
  action = pipe->action;
  
  /* pass this signal to the function or ignore it depending on sa_handler */
  if (handler_func != NULL) {
    // handle it
    action.sa_handler = handler_func;
    action.sa_flags = 0;
  }
  else {
    // ignore it
    action.sa_handler = SIG_IGN;
  }
  sigaction(SIGPIPE, &action, NULL);
}

/*
 * sigpipe_restore() puts back the outside world's opinion of signal handler
 * and SIGPIPE handling. It MUST only be called after a corresponding
 * sigpipe_ignore() was used.
 */
static void sigpipe_restore(struct sigpipe *pipe) {
  /* restore the outside state */
  sigaction(SIGPIPE, &pipe->action, NULL);
}

#else

/* for systems without sigaction */
#define sigpipe_ignore(x, y)
#define sigpipe_restore(x)
#define SIGPIPE_VARIABLE(x)
#endif
#endif
