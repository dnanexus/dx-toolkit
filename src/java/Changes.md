# Java API Bindings Changelog

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
