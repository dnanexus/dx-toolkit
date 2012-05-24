#include "Resolver.h"
#include "dxcpp/dxcpp.h"

vector<string> Split(const string &s, char delim)
{
  vector<string> results;
  string token = "";
  for (unsigned i = 0; i != s.size(); ++i)
  {
    if (s[i] == delim)
    {
      results.push_back(token);
      token = "";
    }
    else
    {
      token += s[i];
    }
  }
  results.push_back(token);

  return results;
}

Path::Path(const string &path_, const string &default_project)
{
  string path = path_;
  vector<string> chunks = Split(path, ':');
  if (chunks.size() == 1)
    path = chunks[0];
  else if (chunks.size() == 2)
  {
    project = chunks[0];
    path = chunks[1];
  }
  else
    throw "Too many ':' in path '" + path + "'";

  folder = "/";
  chunks = Split(path, '/');
  for (unsigned i = 0; i != chunks.size(); ++i)
  {
    if (name != "")
      folder += (folder[folder.size()-1] != '/' ? "/" : "") + name;
    name = chunks[i];
  }

  if (name == "")
    throw "Empty name in path '" + path + "'";

  if (project == "")
    project = default_project;
}

string Path::toString()
{
  return project + ":" + folder + (folder[folder.size()-1] == '/' ? "" : "/") + name;
}

Path Resolver::ParsePath(const string &path)
{
  return Path(path, default_project);
}

string Resolver::FindProject(const string &project)
{
  AutoLock pcl(project_cache_lock);
  if (project_cache.find(project) != project_cache.end())
    return project_cache[project];
  pcl.Release();

  string project_id = LookupProject(project);
  if (project_id != "")
  {
    pcl.Acquire();
    project_cache[project] = project_id;
  }
  return project_id;
}

string Resolver::EnsureProject(const string &project)
{
  string project_id = FindProject(project);
  if (project_id == "")
    project_id = CreateProject(project);

  AutoLock pcl(project_cache_lock);
  project_cache[project] = project_id;
  return project_id;
}

Path Resolver::FindPath(const string &path)
{
  AutoLock pcl(path_cache_lock);
  if (path_cache.find(path) != path_cache.end())
    return path_cache[path];
  pcl.Release();

  Path p = ParsePath(path);
  p.project_id = FindProject(p.project);
  if (p.project_id == "")
    return p;

  p.object_id = LookupPath(p.project_id, p.name, p.folder);
  if (p.object_id != "")
  {
    pcl.Acquire();
    path_cache[path] = p;
  }
  return p;
}

Path Resolver::PreparePath(const string &path)
{
  Path p = FindPath(path);
  if (p.project_id == "")
    p.project_id = EnsureProject(p.project);

  if (p.object_id != "")
  {
    DeleteObject(p.project_id, p.object_id);
    p.object_id = "";
    AutoLock pcl(path_cache_lock);
    path_cache.erase(path);
  }
  return p;
}

string Resolver::LookupProject(const string &project)
{
  if ((project.size() == 32) && (project.substr(0, 8) == "project-"))
    return project;

  dx::JSON results = DXHTTPRequest("/system/findProjects", "{\"describe\": true}")["results"];
  for (unsigned i = 0; i != results.size(); ++i)
    if (results[i]["describe"]["name"].get<string>() == project)
      return results[i]["id"].get<string>();

  return "";
}

string Resolver::LookupPath(const string &project_id, const string &name, const string &folder)
{
  dx::JSON input = dx::JSON(dx::JSON_HASH);
  input["name"] = name;
  input["visibility"] = "either";
  input["scope"] = dx::JSON(dx::JSON_HASH);
  input["scope"]["project"] = project_id;
  input["scope"]["folder"] = folder;
  input["scope"]["recurse"] = true;
  dx::JSON results = DXHTTPRequest("/system/findDataObjects", input.toString());
  if (results["results"].size() == 0)
    return "";

  return results["results"][0]["id"].get<string>();
}

void Resolver::DeleteObject(const string &project_id, const string &object_id)
{
  DXHTTPRequest("/" + project_id + "/removeObjects", "{\"objects\": [\"" + object_id + "\"], \"includeOrphanedHiddenLinks\": false}");
}

string Resolver::CreateProject(const string &name)
{
  dx::JSON input = dx::JSON(dx::JSON_HASH);
  input["name"] = name;
  return DXHTTPRequest("/project/new", input.toString())["id"].get<string>();
}
