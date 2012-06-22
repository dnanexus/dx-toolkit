#include "chunk.h"

using namespace std;

ostream &operator<<(ostream &out, const Chunk &chunk) {
  out << "[" << chunk.localFile << ":" << chunk.start << "-" << chunk.end
      << " -> " << chunk.fileID
      << ", tries=" << chunk.triesLeft << ", data.size()=" << chunk.data.size()
      << "]";
  return out;
}
