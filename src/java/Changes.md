# Java API Bindings Changelog

## 0.153.0

* Raise Java compatibility level from 6 to 7

## 0.152.0

* DescribeOptions.withCustomFields allows the client to specify a
  smaller set of fields to retrieve when describing a data object.
* DataObjectState may now additionally have the value ABANDONED (applies
  to files only).

## 0.126.0

* FindDataObjectsRequestBuilder.withTypes allows constructing complex
  (and/or) queries on object types

## 0.100.0

* Retry logic is now consistent with that of Python bindings, including support
  for Retry-After. The wrappers DXHTTPRequest.request(String, String) and
  DXHTTPRequest.request(String, JsonNode) have been deprecated in favor of
  variants in which the caller must explicitly specify whether the request is
  safe to retry. The previous default behavior was to assume that the call is
  safe to retry (which, as a policy, is unsafe).

## 0.90.0

* DXSearch.findDataObjects and findExecutions support a withIdsIn method that
  lets you provide a list of objects whose IDs will be matched. When used in
  conjunction with includeDescribeOutput this can be used to describe a large
  number of objects at once given their IDs.

## 0.89.0

* DXEnvironment.Builder supports initializing a new builder from an arbitrary
  JSON file or an existing environment.
* DXEnvironment.Builder#setBearerToken lets you set the bearer token (as a
  String) instead of having to build, and then set, the entire security context
  JSON.
* DXEnvironment.Builder supports setting, and DXEnvironment supports getting,
  all the other fields that are stored, too.
* DxSearch.PropertiesQuery allows for complex queries on properties, analogous
  to the functionality for tags.

## 0.88.0

* BREAKING: DXSearch.findExecutions generalizes and replaces findJobs. The
  following new fields are supported for finding executions: class,
  includeSubjobs, name, executable, tags, properties, rootExecution, originJob,
  parentJob, parentAnalysis, state. Obtaining describe output alongside the
  results is possible as well with includeDescribeOutput. Although this change
  is breaking-- classes previously named FindJobs* are renamed to
  FindExecutions*-- if you didn't hold any references to FindJobsRequestBuilder
  or FindJobsResult (i.e. you formulated and executed your query in one line)
  no changes are likely needed to your code. If you did hold such references,
  change instances of FindJobsRequestBuilder and FindJobsResult to
  FindExecutionsRequestBuilder<DXJob> and FindExecutionsResult<DXJob>
  respectively.

* DXExecution now provides factory methods getInstance and
  getInstanceWithEnvironment that return a DXJob or DXAnalysis, as appropriate,
  given its ID.

* DXSearch.findDataObjects now supports compound queries on tags for parity
  with the new functionality in findExecutions. Use the class
  DXSearch.TagsQuery to create such queries.

* DXDataObject.Builder.addTags and addTypes now accept any Collection<String>
  instead of just a List, and they may now be called more than once to append
  to the tags/types to be set.

* DXJob.Describe and DXAnalysis.Describe now support getProperties and getTags
  for retrieving job metadata.

* ExecutableRunner now supports addTags, putProperty, and putAllProperties for
  setting executable metadata.

* ExecutableRunner.dependsOn now accepts any DXExecution instead of only DXJob
  objects.

* The following methods in ExecutableRunner were renamed to be consistent with
  those in DXDataObject.Builder (delegate methods are left for compatibility):
  inFolder -> setFolder, inProject -> setProject, withDetails -> setDetails,
  withInput -> setInput, withName -> setName, withRawInput -> setRawInput.

* DNAnexus object classes now reject malformed object IDs at initialization
  time.

* DXSearch.ObjectProducer<T> now implements Iterable<T>, which lazily loads the
  results when you iterate through them. As before, you can use asList if you
  want to buffer all the results up before retrieving them.

* DXJob.Describe now allows access to most fields provided by the API.
  DXAnalysis.Describe also gets the subset of those fields that applied to
  analyses.

## 0.86.0

* DXSearch.FindDataObjectsRequestBuilder supports requesting describe data with
  the find call and DXDataObject.getCachedDescribe provides access to this
  data.
* DXDataObject.getDetails and .getProperties now throw IllegalStateException if
  the details and properties were not requested with the describe (or find)
  call.

## 0.84.0

* DXAnalysis is now a final class.

## 0.81.0

* ExecutableRunner supports withDetails (for setting details on the resulting
  job) and DXJob.Describe supports getDetails.
* Fix exception classes for some methods of ExecutableRunner.

## 0.79.0

* New classes: DXFile, DXGTable, DXApplet, DXWorkflow (remaining data object
  classes)
* New classes: DXApplet, DXJob, DXAnalysis (executables and executions)
* New low-level bindings with automatic (de)serialization to user-provided
  classes

## 0.78.0

* DXSearch.FindDataObjectsRequestBuilder (frontend to findDataObjects) supports
  many more query parameters.

## 0.77.0

* New classes: DXJob, DXDataObject, DXRecord, DXSearch

## 0.75.0

* BREAKING: The DXAPI class may no longer be instantiated. You must use its
  methods statically.
* New classes: DXObject, DXProject, and DXContainer

## 0.74.0

* `DXHTTPRequest` (and by extension, all the `DXAPI` wrapper methods) have been
  changed to throw only unchecked exceptions. The exception classes that API
  clients will want to take note of are:
  * `DXAPIException` represents an API error; `DXAPIException` has subclasses
    mapping to the various API-defined exception types: InvalidInput, etc.
  * `DXHTTPException` represents HTTP protocol-level problems encountered while
    making the request; these are automatically retried up to 5 times, so you
    will only encounter a `DXHTTPException` in the event of sustained
    connectivity problems or repeated request failures.

## 0.73.0

* New class: DXJSON.ArrayBuilder provides a builder interface for creating JSON
  arrays.
