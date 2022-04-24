import os
from functools import partial
import io
from typing import Any, Optional, Iterator
from pathlib import Path

from . import (
    ffi, dll7z, max_sig_size, formats, log, C, VARTYPE, extensions,
    free_string,
)
from . import py7ziptypes
from .py7ziptypes import ArchiveProps, OperationResult

from .winhelpers import uuid2guidp, get_prop_val, RNOK

from .open_callback import ArchiveOpenCallback
from .extract_callback import (
    ArchiveExtractToDirectoryCallback,
    ArchiveExtractToStreamCallback, ArchiveExtractCallback,
)
from .stream import FileInStream, WrapInStream
from .cmpcodecsinfo import CompressCodecsInfo
from .wintypes import HRESULT


class ExtractionError(Exception):
    def __init__(self, res: OperationResult):
        self.res = res
        self.msg = 'Extraction failed with code: {}'.format(res)
        

class FormatGuessError(Exception):
    def __init__(self):
        super().__init__('failed to guess format')


class Archive:
    archive = None

    def __init__(self, filename: os.PathLike, stream=None, in_stream=None, forcetype: str=None, password: str=None):
        self.password = password
        self.tmp_archive = ffi.new('void**')
        self._path2idx = {}
        self._idx2itm = {}
        self._num_items = None
        iid = uuid2guidp(py7ziptypes.IID_IInArchive)

        filename = Path(filename)
        if not in_stream:
            self.stream = FileInStream(stream or filename)
            stream_inst = self.stream.instances[py7ziptypes.IID_IInStream]
        else:
            self.stream = None
            stream_inst = in_stream

        if forcetype is not None:
            type_name = forcetype
            format = formats[forcetype]
        else:
            type_name, format = self._guess_format(filename, stream_inst)

        self.type_name = type_name
        classid = uuid2guidp(format.classid)

        log.debug('Create Archive (class=%r, iface=%r)',
                  format.classid,
                  py7ziptypes.IID_IInArchive)

        RNOK(dll7z.CreateObject(classid, iid, self.tmp_archive))
        assert self.tmp_archive[0] != ffi.NULL
        self.archive = archive = ffi.cast('IInArchive*', self.tmp_archive[0])
        archive.vtable.AddRef(archive)

        assert archive.vtable.GetNumberOfItems != ffi.NULL
        assert archive.vtable.GetProperty != ffi.NULL

        log.debug('creating callback obj')
        self.open_cb = callback = ArchiveOpenCallback(password=password)
        self.open_cb_i = callback_inst = callback.instances[py7ziptypes.IID_IArchiveOpenCallback]


        set_cmpcodecsinfo_ptr = ffi.new('void**')
        archive.vtable.QueryInterface(
            archive, uuid2guidp(py7ziptypes.IID_ISetCompressCodecsInfo), set_cmpcodecsinfo_ptr)

        if set_cmpcodecsinfo_ptr != ffi.NULL and set_cmpcodecsinfo_ptr[0] != ffi.NULL:
            log.debug('Setting Compression Codec Info')
            self.set_cmpcodecs_info = set_cmpcodecsinfo = \
                ffi.cast('ISetCompressCodecsInfo*', set_cmpcodecsinfo_ptr[0])

            #TODO...
            cmp_codec_info = CompressCodecsInfo()
            cmp_codec_info_inst = cmp_codec_info.instances[py7ziptypes.IID_ICompressCodecsInfo]
            set_cmpcodecsinfo.vtable.SetCompressCodecsInfo(set_cmpcodecsinfo, cmp_codec_info_inst)
            log.debug('compression codec info set')
        else:
            self.set_cmpcodecs_info = None

        #old_vtable = archive.vtable
        log.debug('opening archive')
        maxCheckStartPosition = ffi.new('uint64_t*', 1 << 22)
        RNOK(archive.vtable.Open(archive, stream_inst, maxCheckStartPosition, callback_inst))
        self.itm_prop_fn = partial(archive.vtable.GetProperty, archive)
        #log.debug('what now?')

        #import pdb; pdb.set_trace()
        #archive.vtable = old_vtable
        #tmp_archive2 = ffi.new('void**')
        #RNOK(self.archive.vtable.QueryInterface(archive, iid, tmp_archive2))
        #self.archive = archive = ffi.cast('IInArchive*', tmp_archive2[0])
        #self.tmp_archive = tmp_archive2

        assert archive.vtable.GetNumberOfItems != ffi.NULL
        assert archive.vtable.GetProperty != ffi.NULL
        log.debug('successfully opened archive')

    def _formats_by_path(self, path: Path) -> Iterator[str]:
        for suffix in reversed(path.suffixes):
            names = extensions.get(suffix.lstrip('.'), None)
            if names is not None:
                yield from names

    def _guess_format(self, filename: Path, in_stream):
        log.debug('guess format')

        format_names = set(self._formats_by_path(filename))
        # FIXME: sync to 7-zip preference
        if 'Iso' in format_names and 'Udf' in format_names:
            # UDF is preferred over ISO
            format_names = list(format_names)
            iso_idx = format_names.index('Iso')
            udf_idx = format_names.index('Udf')
            if iso_idx < udf_idx:
                format_names[iso_idx], format_names[udf_idx] = \
                    format_names[udf_idx], format_names[iso_idx]
        file = WrapInStream(in_stream)
        file.seek(0)
        sigcmp = file.read(max_sig_size)
        file.seek(0)
        del file

        for name in format_names:
            format = formats[name]
            if format.start_signature and sigcmp.startswith(format.start_signature):
                log.info('guessed file format: %s' % name)
                return name, format

        for name in format_names:
            format = formats[name]
            log.info('guessed file format: %s' % name)
            return name, format

        for name, format in formats.items():
            if name in format_names:
                continue
            if format.start_signature and sigcmp.startswith(format.start_signature):
                log.info('guessed file format: %s' % name)
                return name, format

        raise FormatGuessError()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        log.debug('Archive.close()')
        if self.archive is None:
            return
        if not self.archive or not self.archive.vtable or self.archive.vtable.Close == ffi.NULL:
            log.warn('close failed, NULLs')
            return
        RNOK(self.archive.vtable.Close(self.archive))
        self.archive.vtable.Release(self.archive)
        self.archive = None

    def __len__(self):
        if self._num_items is None:
            num_items = ffi.new('uint32_t*')
            #import pdb; pdb.set_trace()
            assert self.archive.vtable.GetNumberOfItems != ffi.NULL
            RNOK(self.archive.vtable.GetNumberOfItems(self.archive, num_items))
            log.debug('num_items=%d', int(num_items[0]))
            self._num_items = int(num_items[0])

        return self._num_items

    def get_by_index(self, index):
        log.debug('Archive.get_by_index(%d)', index)
        try:
            return self._idx2itm[index]
        except KeyError:
            itm = ArchiveItem(self, index)
            self._idx2itm[index] = itm
            return itm

    def __getitem__(self, index):
        if isinstance(index, int):
            if index > len(self):
                raise IndexError(index)
            return self.get_by_index(index)
        else:
            if index not in self._path2index:
                found_path = False
                for item in self:
                    if item.path == index:
                        self._path2index[index] = item.index
                        found_path = True
                if not found_path:
                    raise KeyError(index)
            return self.get_by_index(self._path2index[index])

    def __iter__(self):
        log.debug('iter(Archive)')
        for i in range(len(self)):
            yield self[i]
        #isdir = get_bool_prop(i, py7ziptypes.kpidIsDir, self.itm_prop_fn)
        #path = get_string_prop(i, py7ziptypes.kpidPath, self.itm_prop_fn)
        #crc = get_hex_prop(i, py7ziptypes.kpidCRC, self.itm_prop_fn)
        #yield isdir, path, crc

    def __getattr__(self, attr):
        propid = getattr(py7ziptypes.ArchiveProps, attr)
        return get_prop_val(
            partial(self.archive.vtable.GetArchiveProperty,
                    self.archive, propid))

    @property
    def arc_props_len(self) -> int:
        num = ffi.new('uint32_t*')
        RNOK(self.archive.vtable.GetNumberOfArchiveProperties(self.archive, num))
        return int(num[0])

    def get_arc_prop_info(self, index: int) -> tuple[Optional[str], ArchiveProps, VARTYPE]:
        propid = ffi.new('PROPID*')
        vt = ffi.new('VARTYPE*')
        name = ffi.new('wchar_t**')
        try:
            RNOK(self.archive.vtable.GetArchivePropertyInfo(self.archive, index, name, propid, vt))
            if name[0] != ffi.NULL:
                name_str = ffi.string(name[0])
            else:
                name_str = None
        finally:
            if name[0] != ffi.NULL:
                free_string(name[0])
        return name_str, ArchiveProps(propid[0]), VARTYPE(vt[0])

    def iter_arc_props_info(self) -> Iterator[tuple[Optional[str], ArchiveProps, VARTYPE]]:
        for i in range(self.arc_props_len):
            yield self.get_arc_prop_info(i)

    def iter_arc_props(self) -> Iterator[tuple[Optional[str], ArchiveProps, VARTYPE, Any]]:
        for name, prop, vt in self.iter_arc_props_info():
            val = get_prop_val(partial(self.archive.vtable.GetArchiveProperty, self.archive, prop))
            yield name, prop, vt, val

    @property
    def props_len(self) -> int:
        num = ffi.new('uint32_t*')
        RNOK(self.archive.vtable.GetNumberOfProperties(self.archive, num))
        return int(num[0])

    def get_prop_info(self, index: int) -> tuple[Optional[str], ArchiveProps, VARTYPE]:
        propid = ffi.new('PROPID*')
        vt = ffi.new('VARTYPE*')
        name = ffi.new('wchar_t**')
        try:
            RNOK(self.archive.vtable.GetPropertyInfo(self.archive, index, name, propid, vt))
            if name[0] != ffi.NULL:
                name_str = ffi.string(name[0])
            else:
                name_str = None
        finally:
            if name[0] != ffi.NULL:
                free_string(name[0])
        return name_str, ArchiveProps(propid[0]), VARTYPE(vt[0])

    def iter_props_info(self) -> Iterator[tuple[Optional[str], ArchiveProps, VARTYPE]]:
        for i in range(self.props_len):
            yield self.get_prop_info(i)

    def extract(self, directory='', password=None):
        log.debug('Archive.extract()')
        '''
                IInArchive::Extract:
                indices must be sorted
                numItems = 0xFFFFFFFF means "all files"
                testMode != 0 means "test files without writing to outStream"
        '''

        password = password or self.password

        callback = ArchiveExtractToDirectoryCallback(self, directory, password)
        self.extract_with_callback(callback)

    def extract_with_callback(self, callback: ArchiveExtractCallback):
        callback_inst = callback.instances[py7ziptypes.IID_IArchiveExtractCallback]
        assert self.archive.vtable.Extract != ffi.NULL
        RNOK(self.archive.vtable.Extract(self.archive, ffi.NULL, 0xFFFFFFFF, 0, callback_inst))
        if callback.res != OperationResult.kOK:
            raise ExtractionError(callback.res)


class ArchiveItem():
    def __init__(self, archive, index):
        self.archive = archive
        self.index = index
        self._contents = None
        self.password = None

    def extract(self, file, password=None):
        password = password or self.password or self.archive.password

        self.callback = callback = ArchiveExtractToStreamCallback(file, self.index, password)
        self.cb_inst = callback_inst = callback.instances[py7ziptypes.IID_IArchiveExtractCallback]
        indices = ffi.new('uint32_t[]', [self.index])

        log.debug('starting extract of %s!', self.path)
        RNOK(self.archive.archive.vtable.Extract(self.archive.archive, indices, 1, 0, callback_inst))
        log.debug('finished extract')
        if callback.res != OperationResult.kOK:
            raise ExtractionError(callback.res)
    #C.free(indices_p)

    @property
    def contents(self):
        #import pdb; pdb.set_trace()
        if self._contents is None:
            stream = io.BytesIO()
            self.extract(stream)
            self._contents = stream.getvalue()

        return self._contents

    def __getattr__(self, attr):
        propid = getattr(py7ziptypes.ArchiveProps, attr)
        return get_prop_val(partial(self.archive.itm_prop_fn, self.index, propid))

    def iter_props(self) -> Iterator[tuple[Optional[str], ArchiveProps, VARTYPE, Any]]:
        for name, prop, vt in self.archive.iter_props_info():
            val = get_prop_val(partial(self.archive.itm_prop_fn, self.index, prop))
            yield name, prop, vt, val

    def get_seq_in_stream(self) -> Optional[Any]:
        get_void_ptr = ffi.new('void**')
        archive = self.archive.archive
        res = archive.vtable.QueryInterface(
            archive, uuid2guidp(py7ziptypes.IID_IInArchiveGetStream), get_void_ptr)
        if res != HRESULT.S_OK.value or get_void_ptr[0] == ffi.NULL:
            return None
        get_stream = ffi.cast('IInArchiveGetStream*', get_void_ptr[0])
        get_void_ptr[0] = ffi.NULL
        get_sub_seq_stream_ptr = ffi.new('ISequentialInStream**')
        res = get_stream.vtable.GetStream(get_stream, self.index, get_sub_seq_stream_ptr)
        if res != HRESULT.S_OK.value or get_sub_seq_stream_ptr[0] == ffi.NULL:
            return None
        sub_seq_stream = ffi.cast('ISequentialInStream*', get_sub_seq_stream_ptr[0])
        sub_seq_stream.vtable.AddRef(sub_seq_stream)
        return sub_seq_stream

    def get_in_stream(self) -> Optional[Any]:
        get_void_ptr = ffi.new('void**')
        seq_in_stream = self.get_seq_in_stream()
        if seq_in_stream is None:
            return None
        res = seq_in_stream.vtable.QueryInterface(
            seq_in_stream, uuid2guidp(py7ziptypes.IID_IInStream), get_void_ptr)
        if res != HRESULT.S_OK.value or get_void_ptr[0] == ffi.NULL:
            seq_in_stream.vtable.Release(seq_in_stream)
            return None
        in_stream = ffi.cast('IInStream*', get_void_ptr[0])
        return in_stream
