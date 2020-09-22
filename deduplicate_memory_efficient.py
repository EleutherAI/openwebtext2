from datasketch import MinHashLSH
import pickle
import sys
import tqdm

from utils import Timer

def perform_minhash_lsh_dedupe(minhashes, output_pickle_path):
    # [(file_name, [(file_index, minhash), .....]), ....]

    total_documents = 0
    for _ , documents in minhashes:
        total_documents += len(documents)              

    print("Building minhash LSH")
    timer = Timer().start()
    lsh = MinHashLSH(threshold=0.5, num_perm=128)
    progress = tqdm.tqdm(total=total_documents, dynamic_ncols=True, unit_scale=1)
    for file_id, (file_name, documents) in enumerate(minhashes):
        for document_id, minhash in enumerate(documents):
            lsh.insert((file_id, document_id), minhash)
            progress.update()
    progress.close()
    print(timer.stop_string())

    print("Building garbage list")
    timer.start()
    garbage_docs = [(file_name, []) for file_name in minhashes]
    progress = tqdm.tqdm(total=total_documents, dynamic_ncols=True, unit_scale=1)
    for file_id, (file_name, documents) in enumerate(minhashes):
        for document_id, minhash in enumerate(documents):            
            if lsh.query(minhash):
                garbage_docs[file_id][1].append(document_id)
                lsh.remove((file_id, document_id))
            progress.update()
    progress.close()
    print(timer.stop_string())

    print("Pickling Garbage. Mmmmmmm")
    timer.start()
    pickle.dump(file_garbage_set, open(output_pickle_path, "wb"))
    print(timer.stop_string())

if __name__ == '__main__':
    minhashes_pickle_path = "E:\Eleuther_AI\webtext2\dedupe\minhashes.pkl"
    garbage_pickle_path = "E:\Eleuther_AI\webtext2\dedupe\garbage.pkl"

    print("Unpickling minhashes")
    timer = Timer().start()
    minhashes = pickle.load(open(minhashes_pickle_path, "rb"))
    print(timer.stop_string())

    perform_minhash_lsh_dedupe(minhashes, garbage_pickle_path)

        

