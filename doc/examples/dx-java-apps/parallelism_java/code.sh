#!/bin/bash
set -e

export CLASSPATH=${DX_FS_ROOT}/:$CLASSPATH

# Main entry point for this app.
main() {
  java DXParallelismExample
}

# Entry point for parallel subtasks.
process() {
  echo "Starting PROCESS subtask $index"

  # Download the input files.
  input_file_id=$(dx-jobutil-parse-link "$input_file")
  dx download "$input_file_id" -o input_file --no-progress

  # (Replace this with an actual analysis step that reads from input_file and
  # outputs to output_file.)
  cat input_file > output_file

  # Upload the output files and add the output.
  output_file_id=`dx upload output_file --brief --no-progress`
  dx-jobutil-add-output output_file "$output_file_id"

  echo "Finished PROCESS subtask $index"
}

# This entry point is run after all 'process' tasks have finished.
postprocess() {
  echo "Starting POSTPROCESS subtask"

  # Replace this with whatever work is needed to combine the work from the
  # PROCESS stages to make the final result.
  #
  # In this case we just concatenate all the files, so we end up with the
  # original input file repeated numSubtasks times.
  for process_output in "${process_outputs[@]}"
  do
    process_output_id=$(dx-jobutil-parse-link "$process_output")
    dx cat $process_output_id >> combined_output
  done

  # Upload the output files and add the output.
  combined_output_id=`dx upload combined_output --brief --no-progress`
  dx-jobutil-add-output combined_output "$combined_output_id"
}
