#include <gdbm.h>

int junk_gdbm_store(GDBM_FILE dbf, int klen, char* kptr, int vlen, char* vptr, int n)
{
	datum k = { kptr, klen };
	datum v = { vptr, vlen };
	return gdbm_store(dbf, k, v, n);
}

char *junk_gdbm_fetch(GDBM_FILE dbf, int klen, char* kptr, int* vlen)
{
	datum k = { kptr, klen };
	datum v = gdbm_fetch(dbf, k);
	*vlen = v.dsize;
	return v.dptr;
}

int junk_gdbm_delete(GDBM_FILE dbf, int klen, char* kptr)
{
	datum k = { kptr, klen };
	return gdbm_delete(dbf,k);
}

char* junk_gdbm_firstkey(GDBM_FILE dbf, int *klen)
{
	datum k = gdbm_firstkey(dbf);
	*klen = k.dsize;
	return k.dptr;
}

char* junk_gdbm_nextkey(GDBM_FILE dbf, int klen, char* kptr, int* new_klen)
{
	datum k = { kptr, klen };
	datum new_k = gdbm_nextkey(dbf, k);
	*new_klen = new_k.dsize;
	return new_k.dptr;
}
