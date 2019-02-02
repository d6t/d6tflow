
import hashlib
def filemd5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(int(1e7)), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def dfchunk(df, chunk_size=1e6):
    chunk_size = int(chunk_size)
    for istart in range(0,df.shape[0],chunk_size):
        yield (df.iloc[istart:istart + chunk_size,])


def dfmd5(df, chunk_size=1e6):
    hash_md5 = hashlib.md5()
    for chunk in dfchunk(df,chunk_size):
        hash_md5.update(str(chunk).encode('utf-8'))
    return hash_md5.hexdigest()

