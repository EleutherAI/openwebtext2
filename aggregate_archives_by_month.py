import os
import glob
import tqdm

import lm_dataformat
import zstandard
def commit_override_path(self, archive_path):
    self.compressor.flush(zstandard.FLUSH_FRAME)
        
    self.fh.flush()
    self.fh.close()
    os.rename(self.out_dir + '/current_chunk_incomplete', archive_path)
    self.fh = open(self.out_dir + '/current_chunk_incomplete', 'wb')
    self.compressor = self.cctx.stream_writer(self.fh)

    self.i += 1

lm_dataformat.Archive.commit_override_path = commit_override_path

if __name__ == '__main__':
    scrapes_directory = "E:/Eleuther_AI/webtext2/scrapes"

    for scrape_subdirectory in glob.glob(os.path.join(scrapes_directory,"*")):
        if os.path.isdir(scrape_subdirectory):
            print(f"Aggregating archives from {scrape_subdirectory}")
            aggregate_file_name = f"{os.path.basename(scrape_subdirectory)}.jsonl.zst"
            aggregate_file_path = os.path.join(scrapes_directory, aggregate_file_name)

            lm_archiver = lm_dataformat.Archive(scrapes_directory)

            for archive_file in tqdm.tqdm(glob.glob(f'{scrape_subdirectory}/*jsonl.zst')):
                lm_reader = lm_dataformat.Reader("")
                for text, meta in lm_reader.read_jsonl(archive_file, get_meta=True):
                    lm_archiver.add_data(text, meta)

            lm_archiver.commit_override_path(aggregate_file_path)