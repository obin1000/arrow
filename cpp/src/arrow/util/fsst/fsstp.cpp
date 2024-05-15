#include "fsstp.hpp"
#include "fsst.h"
#include "fsstp_no_queues.hpp"
#include "fsstp_split_block.hpp"
#include "fsstp_thread_per_block.hpp"

#include "fsstp_kernel.h"

#include <chrono>
#include <iterator>
#include <unistd.h>

using namespace std;


vector<unsigned char> read_file_multiple_times(ifstream &src, unsigned int times) {
   size_t reads_size = 100000;
   std::vector<unsigned char> data(2000000000);
   size_t size = 0;
   for (int i = 0; i < times; i++) {
      while (true) {
         src.read(reinterpret_cast<char *>(data.data() + size), reads_size);
         auto count = src.gcount();
         size += count;
         if (count < reads_size)
            break;
      }
      src.clear();
      src.seekg(0, std::ios::beg);
   }
   data.resize(size);
   return data;
}


vector<unsigned char> read_file_chunked(ifstream &src) {
   size_t reads = 100000;
   std::vector<unsigned char> data(4000000000);
   size_t size = 0;
   while (true) {
      src.read(reinterpret_cast<char *>(data.data() + size), reads);
      auto count = src.gcount();
      size += count;
      if (count < reads)
         break;
   }
   data.resize(size);
   return data;
}

vector<unsigned char> read_file_direct(ifstream &src) {
   std::vector<unsigned char> fileContents;
   fileContents.reserve(2000000000);
   fileContents.assign(std::istreambuf_iterator<char>(src), std::istreambuf_iterator<char>());
   return fileContents;
}

void print_usage() {
   std::cerr << "Usage: fsstp [-d] [-b block size] [-t number of threads] <input file> <output file>\n";
}

int main(int argc, char *argv[]) {
   unsigned int number_of_threads = 7;
   size_t block_size = (1ULL << 20);
   bool decompress = false;
   const unsigned int method = 3;

   int opt;

   while ((opt = getopt(argc, argv, "db:t:")) != -1) {
      switch (opt) {
         case 'd':
            decompress = true;
            break;
         case 'b':
            block_size = atoi(optarg);
            break;
         case 't':
            number_of_threads = atoi(optarg);
            break;
         default:
            print_usage();
            return 1;
      }
   }
   if (argc - optind != 2) {
      print_usage();
      return 1;
   }

   string srcfile(argv[optind]);
   string dstfile(argv[optind + 1]);


   ifstream src;
   ofstream dst;
   src.open(srcfile, ios::binary);
   dst.open(dstfile, ios::binary);
   dst.exceptions(ios_base::failbit);
   dst.exceptions(ios_base::badbit);
   src.exceptions(ios_base::badbit);


   if (decompress) {
      if (method == 1) {
         fsstp::decompress_file(src, dst, number_of_threads);
      }
      if (method == 2) {
         auto data = read_file_chunked(src);
         std::vector<unsigned char> outbuf(4 * data.size());
         size_t size;
         unsigned int repeats = 5;
         for (unsigned int i = 0; i < repeats; i++) {
            auto start = chrono::high_resolution_clock::now();
            size = fsstp::no_queues::decompress_buffer(data.data(), data.size(), outbuf.data(), number_of_threads);
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            cout << (decompress ? "Dec" : "C") << "ompressed in " << duration.count() / 1000 << " miliseconds" << endl;
         }
         dst.write(reinterpret_cast<const char *>(outbuf.data()), size);
      }
      if (method == 3) {
         auto data = read_file_chunked(src);
         const size_t dst_size = 4 * data.size();
         std::vector<unsigned char> outbuf(dst_size);
         size_t size;
         unsigned int repeats = 5;
         for (unsigned int i = 0; i < repeats; i++) {
            auto start = chrono::high_resolution_clock::now();
            size = fsstp::kernel::decompress_buffer(data.data(), data.size(), outbuf.data(), dst_size);
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            cout << (decompress ? "Dec" : "C") << "ompressed total in " << duration.count() / 1000 << " miliseconds"
                 << endl
                 << endl;
         }
         dst.write(reinterpret_cast<const char *>(outbuf.data()), size);
      }
   }
   else {
      if (method == 1) {
         fsstp::compress_file(src, dst, block_size);
      }
      if (method == 2 || method == 3) {
         auto data = read_file_chunked(src);
         const size_t number_of_blocks = 64;
         block_size = (data.size()/number_of_blocks) + 1;
         std::vector<unsigned char> outbuf(4 * data.size());
         auto size = fsstp::compress_buffer(data.data(), data.size(), outbuf.data(), block_size);
         dst.write(reinterpret_cast<const char *>(outbuf.data()), size);
      }
   }


   src.close();
   dst.close();
   return 0;
}
