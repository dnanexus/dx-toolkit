#!/usr/bin/env python
# subprocess_example 0.0.1

import subprocess

import dxpy

@dxpy.entry_point('main')
def main(**kwargs):
    columns = [dxpy.DXGTable.make_column_desc("word", "string")]
    output_gtable = dxpy.new_dxgtable(columns=columns)

    # Call a subprocess and dump its output to a local file.
    # (Remove possessives and other bogus words from the word list)
    subprocess.check_call('egrep "^[a-z]+$" /usr/share/dict/american-english > words.txt', shell=True)

    # Parse the file we just generated into a GTable.
    for word in open("words.txt"):
        output_gtable.add_row([word.strip()])

    output_gtable.close()

    return {'words': dxpy.dxlink(output_gtable.get_id())}

dxpy.run()
