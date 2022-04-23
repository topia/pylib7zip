import sys
import uuid

IID_IUnknown = uuid.UUID('{00000000-0000-0000-C000-000000000046}')

CDEF_IUnknown = """
	HRESULT (*QueryInterface) (void*, GUID*, void**);
	uint32_t (*AddRef)(void*);
	uint32_t (*Release)(void*);
"""

# from https://github.com/harvimt/pylib7zip/pull/5/files by cielavenir
if 'win' not in sys.platform:
	# https://forum.lazarus.freepascal.org/index.php/topic,42701.msg298820.html#msg298820
	CDEF_IUnknown += """
	void* _IUnknown_Reserved1;
	void* _IUnknown_Reserved2;
"""

CDEFS = """
typedef struct {
	""" + CDEF_IUnknown + """
} _IUnknown_vtable;

typedef struct {
	_IUnknown_vtable* vtable;
} IUnknown;
"""
