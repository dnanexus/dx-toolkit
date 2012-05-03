/*
 * Write the contents of a ContigSet object to a local file in FASTA
 * format.
 *
 * Usage: contigset2fasta contigset_id fasta_filename
 */

#include <iostream>

#include "dxcpp/dxcpp.h"

using namespace std;
using namespace dx;

void format_FASTA(string &name, vector<char> &buf, ofstream &out, int line_len = 60) {
  out << ">" << name << endl;
  int64_t length = buf.size();
  for (int64 j = 0; j < length; j += line_len) {
    out.write((const char *) (&(buf[j])), min<int64_t>(line_len, length - j));
    out << endl;
  }
}

void process(const char * contigset_id, const char * fasta_filename) {
  cerr << "- Getting details for ContigSet " << string(contigset_id) << "..." << endl;
  DXRecord contigset(contigset_id);

  JSON details = contigset.getDetails();
  cerr << details.toString() << endl;

  string flatfile_id = details["flat_sequence_file"]["$dnanexus_link"].get<string>();

  cerr << "- Downloading flatfile " << flatfile_id << endl;
  DXFile::downloadDXFile(flatfile_id, "flatfile", 100 * 1024 * 1024);

  ifstream in("flatfile", ifstream::in);
  ofstream out(fasta_filename, ofstream::out | ofstream::trunc);

  JSON &contig_names = details["contigs"]["names"];
  JSON &contig_sizes = details["contigs"]["sizes"];
  JSON &contig_offsets = details["contigs"]["offsets"];
  int num_contigs = contig_names.size();

  for (int i = 0; i < num_contigs; ++i) {
    string name = contig_names[i].get<string>();
    int64_t length = contig_sizes[i].get<int64_t>();
    int64_t offset = contig_offsets[i].get<int64_t>();

    in.seekg(offset);
    vector<char> buf(length);
    in.read(&(buf[0]), length);

    format_FASTA(name, buf, out);
  }

  out.close();
  in.close();
}

int main(int argc, char * argv[]) {
  cerr << "* Starting contigset2fasta..." << endl;

  if (argc != 3) {
    cerr << "Usage: " << argv[0] << " contigset_id fasta_filename" << endl;
    return 1;
  }

  process(argv[1], argv[2]);

  cerr << "* Finished contigset2fasta." << endl;
  return 0;
}
