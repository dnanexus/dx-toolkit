#!/usr/bin/env python
# parallelism_example 0.0.1

import logging

import dxpy

logging.basicConfig(level=logging.DEBUG)

@dxpy.entry_point("main")
def main(words, chunk_size=10000):
    """
    Using a list of words (a GTable with a single string column), produces a
    new GTable with the same words, sorted by the reverse of each word.
    """
    columns = [dxpy.DXGTable.make_column_desc("word", "string"),
               dxpy.DXGTable.make_column_desc("reverse", "string")]
    indices = [dxpy.DXGTable.lexicographic_index([["reverse", "ASC"]], "reverse_order")]
    output_gtable = dxpy.new_dxgtable(columns=columns, indices=indices)

    input_num_rows = dxpy.DXGTable(words).describe()["length"]

    postprocess_job_inputs = {}

    for worker_index, row_offset in enumerate(xrange(0, input_num_rows, chunk_size)):
        logging.debug("Worker %d is processing %d rows starting at row %d" % (worker_index, chunk_size, row_offset))
        mapper_job_inputs = {
            "words": words,
            "row_offset": row_offset,
            "length": chunk_size,
            "output_gtable_id": output_gtable.get_id()
            }
        mapper_job = dxpy.new_dxjob(mapper_job_inputs, "mapper")
        # Add a dependency between each map job and the postprocess job.
        postprocess_job_inputs["worker_%d_result"] = {"job": mapper_job.get_id(), "field": "ok"}

    postprocess_job_inputs["output_gtable_id"] = output_gtable.get_id()
    postprocess_job = dxpy.new_dxjob(postprocess_job_inputs, "postprocess")

    return {"backwards_sorted_words": {"job": postprocess_job.get_id(), "field": "output_gtable"}}

@dxpy.entry_point("mapper")
def mapper(words, output_gtable_id, row_offset, length):
    """
    Processes a contiguous range of the gtable rows.

    Maps each row [word] to a row in the output table [word, word_reversed].
    """
    with dxpy.open_dxgtable(words, keep_open=True) as input_gtable, dxpy.open_dxgtable(output_gtable_id, keep_open=True) as output_gtable:
        # row_offset + length may exceed the length of the input table, but
        # that's not a problem (iterate_rows will truncate its result at the
        # end).
        for index, word in input_gtable.iterate_rows(start=row_offset, end=row_offset+length):
            output_gtable.add_row([word, word[::-1]])
    return {"ok": True}

@dxpy.entry_point("postprocess")
def postprocess(output_gtable_id, **job_inputs):
    """
    Closes the output table.
    """
    # **job_inputs soaks up the arguments worker_0_result, worker_1_result,
    # etc. which we used for dependency management but don't actually need to
    # look at here.
    output_gtable = dxpy.DXGTable(output_gtable_id)
    output_gtable.close()
    return {"output_gtable": dxpy.dxlink(output_gtable_id)}

dxpy.run()
