

#include "arrow/util/compression_internal.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

#include "fsst/fsstp_no_queues.hpp"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// FSST implementation

class FSSTCodec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    size_t decompressed_size = noqueue::decompress_buffer(input,input_len, output_buffer, 8);
    // printf("\nDOUT: %lu %li \n", decompressed_size, input_len);

    return static_cast<int64_t>(decompressed_size);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return input_len;
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t ARROW_ARG_UNUSED(output_buffer_len),
                           uint8_t* output_buffer) override {
    const size_t number_of_blocks = 64;
    const size_t block_size = (input_len/number_of_blocks) + 1;
    size_t output_size = compress_buffer(input, input_len, output_buffer, block_size);
    // printf("\nOUT: %lu %li \n", output_size, input_len);
    return static_cast<int64_t>(output_size);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with FSST");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with FSST");
  }

  Compression::type compression_type() const override { return Compression::FSST; }
  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeFSSTCodec() { return std::make_unique<FSSTCodec>(); }

}  // namespace internal
}  // namespace util
}  // namespace arrow
