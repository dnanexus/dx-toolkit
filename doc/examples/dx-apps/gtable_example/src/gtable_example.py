#!/usr/bin/env python
# gtable_example 0.0.1

import subprocess

import dxpy

@dxpy.entry_point('main')
def main(**kwargs):
    columns = [dxpy.DXGTable.make_column_desc("word", "string")]

    # Call a subprocess and dump its output to a local file.
    # (Remove possessives and other bogus words from the word list)
    subprocess.check_call('egrep "^[a-z]+$" /usr/share/dict/american-english > words.txt', shell=True)

    # Parse the file we just generated into a GTable.
    with dxpy.new_dxgtable(columns=columns, mode='w') as output_gtable:
        for index, word in enumerate(open("words.txt")):
            output_gtable.add_row([word.strip()])
            if index % 10000 == 0:
                print "Read word: " + word.strip()
    # Closing the GTable automatically commences at the conclusion of the "with" block.

    return {'words': dxpy.dxlink(output_gtable.get_id())}

dxpy.run()
