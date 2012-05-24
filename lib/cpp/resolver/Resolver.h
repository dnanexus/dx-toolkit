#ifndef RESOLVER_RESOLVER_H
#define RESOLVER_RESOLVER_H

#include <map>
#include <string>
#include <vector>

#include "Thread.h"

using namespace std;

// Resolver
//
// Resolver is an unofficial, thin layer that facilitates symbolic name resolution.
// Resolver can be used to resolve object paths, and other operations describe below.
//
// An object path is a string of the form: [ <project> ":" ] [ <folder_prefix> ] name
// Examples of object paths:
//
//   hg18
//   /hg18 (same as above)
//   genomes/hg18
//   /genomes/hg18 (same as above)
//   seed:hg18
//   seed:/hg18 (same as above)
//   seed:genomes/hg18
//   seed:/genomes/hg18 (same as above)
//   project-000000000000000000000001:hg18
//
// How to use resolver:
// -------------------
//
// To initialize a resolver with a default project:
//
//   Resolver resolver(default_project);
//   Resolver resolver("seed");
//
// To use the resolver to "parse" a path into parts:
//
//   Path p = resolver.ParsePath("genomes/hg18");
//   cerr << p.project  // Prints "seed" (or whatever you gave to constructor)
//   cerr << p.folder   // Prints "/genomes"
//   cerr << p.name     // Prints "hg18"
//
// To use the resolver to convert from project name to project id:
//
//   // Returns "" if project not found
//   string project_id = resolver.FindProject("seed");
//
// To use the resolver to convert from project name to project id,
// or create a new project if it doesn't already exist:
//
//   // Returns an existing or new project_id
//   string project_id = resolver.EnsureProject("seed");
//
// To use the resolver to convert from object path to object id:
//
//   Path p = resolver.FindPath("genomes/hg18");
//   cerr << p.project    // Prints "seed" (or whatever you gave to constructor)
//   cerr << p.folder     // Prints "/genomes"
//   cerr << p.name       // Prints "hg18"
//   cerr << p.project_id // Prints the project id of "seed" (if the project exists)
//   cerr << p.object_id  // Prints the object id of an object named "hg18" under any folder
//                        // starting from "/genomes" inside project p.project_id (if
//                        // such an object exists)
//
// To use the resolver to "prepare for the creation of a path", that is, to ensure
// that a particular project exists but also that a particular object does NOT
// exist:
//
//   Path p = resolver.PreparePath("genomes/hg18");
//   // Creates project "seed" if it doesn't exist, and removes object "hg18"
//   // if it exists in that project.
//
// Why would you want this? Symbolic lookup is only possible if you have a single
// copy of an object with a particular name, otherwise the lookup is ambiguous.
// Therefore, often if you want to create FOO, you most likely want to remove any
// existing FOOs from the system prior to making FOO (it is equivalent to what
// "command > FOO" would do on UNIX; it always 'overwrites' FOO, and the way to
// emulate this behavior here is by first calling PreparePath before you make
// a new FOO).

class Path
{
  public:

  string project;
  string folder;
  string name;
  string project_id;
  string object_id;

  Path() { }
  Path(const string &path, const string &default_project = "");
  string toString();
};

class Resolver
{
  public:

  Resolver() { }
  Resolver(const string &default_project_) : default_project(default_project_) { }

  Path ParsePath(const string &path);

  string FindProject(const string &project);
  string EnsureProject(const string &project);

  Path FindPath(const string &path);
  Path PreparePath(const string &path);

  private:

  string LookupProject(const string &project);
  string LookupPath(const string &project_id, const string &name, const string &folder);
  void DeleteObject(const string &project_id, const string &object_id);
  string CreateProject(const string &name);

  string default_project;
  map<string, string> project_cache;
  map<string, Path> path_cache;
  Lock project_cache_lock;
  Lock path_cache_lock;
};

#endif
