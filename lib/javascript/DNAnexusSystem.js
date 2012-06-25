var process = require('process');
var fs = require('fs');

/** @brief Execute a given command in a separate process
 *  @publish
 *  This function is used for executing arbitrary commands in a different process
 *  @tparam String cmd The command to be executed
 *  @tparam Hash options A hash containing following boolean options 
 *  (Default is "false" for all options)
 *  @markdown
 *  - _capture\_stdout_: (optional) If set to true, then stdout's output is returned
 *  - _capture\_stderr_: (optional) If set to true, then stderr's ouptut is returned
 *  @endmarkdown
 *  @return A hash with following fields:
 *  @markdown
 *  - _exitCode_: Always present. The exit code returned by the process.
 *  - _stdout_: Present iff boolReturnOutput = true. Contains content of standard output.
 *  - _stderr_: Present iff boolReturnOutput = true. Contains content of standard error.
 *  @endmarkdown
 */
exports.system = function(cmd, options) {
  var p = new process.Process();
  var exitCode;
  var tempFile_stdout = null, tempFile_stderr = null;
  var finalCmd = "(" + cmd + ")";
  var f;
  if (options === undefined) {
    options = {};
  }
  if(options["capture_stdout"] === true) {
    tempFile_stdout = p.exec("mktemp");
    if(tempFile_stdout == null) {
      throw new Error("Unable to execute command mktemp");
    }
    // Remove \n at end of string - since "mktemp" return with a "\n" at the end
    tempFile_stdout = tempFile_stdout.replace(/\n$/g, '');
    finalCmd += " > " + tempFile_stdout;
  }
  else {
    // If output is not required, redirect it to /dev/null
    finalCmd += " > /dev/null";
  }
  if (options["capture_stderr"] === true) {
    tempFile_stderr = p.exec("mktemp");
    if(tempFile_stderr == null) {
      throw new Error("Unable to execute command mktemp");
    }
    tempFile_stderr = tempFile_stderr.replace(/\n$/g, '');
    finalCmd += " 2> " + tempFile_stderr;
  }
  else {
   // Disabling for debugging purposes - stderr output will go to log
   // finalCmd += " 2> /dev/null";
  }

  // An internal function - just for use by DNAnexus.system()
  function clearResources() {
    // Remove temp files for stdout and stderr (if they are created in first place)
    if (tempFile_stdout !== null) {
      f = new fs.File(tempFile_stdout);
      f.remove();
      delete f;
    }
    if (tempFile_stderr !== null) {
      f = new fs.File(tempFile_stderr);
      f.remove();
      delete f;
    }
    delete p;
  }

  system.stderr("Running " + finalCmd + "\n");
  exitCode = p.system(finalCmd);

  if (exitCode !== 0) {
    clearResources();
    throw new Error("The command " + cmd + " exited with non-zero exit code");
  }
  var return_hash = {"exitCode": exitCode};
  if(options["capture_stdout"] === true) {
   f = new fs.File(tempFile_stdout);
   f.open("r");
   return_hash.stdout = f.read().toString("utf-8");
   f.close();
   delete f;
  }

  if(options["capture_stderr"] === true) {
    f = new fs.File(tempFile_stderr);
    f.open("r");
    return_hash.stderr = f.read().toString("utf-8");
    f.close();
    delete f;
  }
  clearResources();

  return return_hash;
}
